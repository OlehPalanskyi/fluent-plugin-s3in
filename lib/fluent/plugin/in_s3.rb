require 'fileutils'
require 'sequel'
require 'aws-sdk'
require 'time'
require 'singleton'
require 'sqlite3'
require 'tzinfo'
require 'zlib'
# Fluent
module Fluent
  # S3Input
  class S3Input < Fluent::Input
    Fluent::Plugin.register_input('s3in', self)

    define_method('router') { Fluent::Engine } unless method_defined?(:router)

    # AWS Common Config
    config_param :region, :string, default: nil
    # AWS Credential Config
    config_param :access_key_id, :string, default: nil
    config_param :secret_access_key, :string, default: nil
    # AWS S3 Config
    config_param :s3_bucket, :string, default: nil
    config_param :s3_prefix, :string, default: nil
    DATE_CONDITION_MAX_NUM = 20
    (1..DATE_CONDITION_MAX_NUM).each do |i|
      config_param "s3_key_date_condition#{i}".to_sym, :string, default: nil
    end
    config_param :s3_key_format, :string, default: nil
    config_param :s3_key_exclude_format, :string, default: nil
    config_param :s3_key_current_format, :string, default: nil
    # Log Format
    config_param :format, :string, default: nil
    config_param :multiline, :bool, default: false
    config_param :format_firstline, :string, default: nil
    config_param :tag, :string, default: 's3in.log'
    config_param :timestamp, :string, default: nil
    config_param :timezone, :string, default: nil
    # Workspace Config
    config_param :work_dir, :string, default: nil
    config_param :clear_db_at_start, :bool, default: false
    # S3 Describe Interval
    config_param :refresh_interval, :integer, default: 300
    config_param :start_now, :bool, default: false
    # EC2 Describe Config
    config_param :add_instance_tags, :bool, default: false
    # Performance Config
    config_param :download_thread_num, :integer, default: 5
    config_param :parse_thread_num, :integer, default: 5

    attr_reader :status
    attr_reader :emit_counter

    module Status
      READY = 1
      RUNNING = 2
      WAITING = 3
      SHUTDOWN = 4
    end

    def initialize
      super
      @status = Status::READY
      @shutdown_flag = false
      @router = router
      @download_queue = Queue.new
      @archive_parse_queue = Queue.new
      @current_parse_queue = Queue.new
      @start_queue = Queue.new
    end

    def configure(conf)
      super

      @emit_counter = 0

      fail 'region is required' if @region.empty?
      if @access_key_id.empty? && @secret_access_key.empty?
        fail 'set "access_key_id" and "secret_access_key", or set "iam role" in this instance' unless _iam_role?
      else
        fail 'access_key_id is required' if @access_key_id.empty?
        fail 'secret_access_key is required' if @secret_access_key.empty?
      end
      fail 's3_bucket is required' if @s3_bucket.empty?
      fail 's3_prefix is required' if @s3_prefix.empty?

      @date_conditions = []
      (1..DATE_CONDITION_MAX_NUM).each do |index|
        next if conf["s3_key_date_condition#{index}"].nil?
        params = conf["s3_key_date_condition#{index}"].split(' ', 2)
        fail "s3_key_date_condition#{index} parse error" if params.size != 2
        @date_conditions << { group_name: params[0], condition_str: params[1] }
      end
      time = Time.now.utc
      @date_conditions.each do |date_condition|
        _validate_date_condition(time_str: nil, condition_str: date_condition[:condition_str], current_time: time)
      end

      @s3_prefix = _add_end_slash(@s3_prefix)
      fail 'Requires "s3_key_format" if input "s3_key_date_condition" values' if @date_conditions.size > 0 && @s3_key_format.empty?
      @s3_key_format_regexp = _regexp_format(format: @s3_key_format, deny_no_name: @date_conditions.size > 0) unless @s3_key_format.empty?
      @s3_key_exclude_format_regexp = _regexp_format(format: @s3_key_exclude_format) unless @s3_key_exclude_format.empty?
      @s3_key_current_format_regexp = _regexp_format(format: @s3_key_current_format) unless @s3_key_current_format.empty?

      fail 'format is required' if @format.empty?
      @format_regexp = _regexp_format(format: @format, deny_no_name: true)
      @format_firstline_regexp = _regexp_format(format: @format_firstline) unless @format_firstline.empty?

      fail 'download_thread_num is required (over 1)' if @download_thread_num <= 0
      fail 'parse_thread_num is required (over 1)' if @parse_thread_num <= 0

      fail 'timezone is required' if @timezone.empty?

      _validate_timestamp unless @timestamp.empty?

      fail 'work_dir is required' if @work_dir.empty?
      @work_dir = _add_end_slash(@work_dir)
      _make_work_dir

      true
    rescue => e
      raise Fluent::ConfigError, "error occurred: #{e.message}, #{e.backtrace.join("\n")}"
    end

    def _validate_timestamp
      params = @timestamp.split(' ', 2)
      fail "not found timestamp group name '#{params[0]}' in format" unless @format_regexp.named_captures.include? params[0]
      @timestamp = {
        group_name: params[0],
        format: params[1].nil? ? nil : params[1]
      }
    end

    def _strptime_with_timezone(date, format = nil)
      utc_offset = @timezone.empty? ? Time.now.utc_offset / 60 : TZInfo::Timezone.get(@timezone).current_period.utc_offset / 60
      offset_ope = (utc_offset >= 0) ? '+' : '-'
      utc_offset = utc_offset.abs
      offset_hour = (utc_offset / 60).to_i
      offset_min = utc_offset - (offset_hour * 60)
      offset_str = sprintf('%s%02d:%02d', offset_ope, offset_hour, offset_min)
      unless (/(%z|%:z|%::z|%Z)/ =~ format)
        if md = date.match(/([\+\-Z])([0-9]{2})?:?([0-9]{2})?$/)
          date = date.gsub(md[0], offset_str)
        end
      end
      time = format.nil? ? Time.parse(date).iso8601 : Time.strptime(date, format).iso8601
      unless (/(%z|%:z|%::z|%Z)/ =~ format)
        if md = time.match(/([\+\-Z])([0-9]{2})?:?([0-9]{2})?$/)
          time = time.gsub(md[0], offset_str)
        end
      end
      Time.iso8601(time)
    end

    def _make_work_dir
      if Dir.exist?(@work_dir)
        fail 'work_dir is not writable' unless FileTest.writable?(@work_dir)
      else
        begin
          FileUtils.mkdir_p
        rescue
          raise 'coud not make work_dir'
        end
      end
    end

    def _validate_date_condition(time_str: nil, condition_str:, current_time:)
      date_format = condition_str.rpartition(' ')[0].rpartition(' ')[0]
      left_side_time = time_str.nil? ? nil : _strptime_with_timezone(time_str, date_format)
      comparison_operator = condition_str.rpartition(' ')[0].rpartition(' ')[2]
      right_side_time = _diff_to_time(condition_str.rpartition(' ')[2], current_time)
      _compare_times(left_side_time, comparison_operator, right_side_time)
    rescue => e
      raise "s3_key_date_condition parse error: #{e.message}"
    end

    def _diff_to_time(diff_str, current_time)
      if md = diff_str.match(/^([\+\-])[0-9]+$/)
        number = diff_str.rpartition(md[1])[2].to_i
        case md[1]
        when '+'
          return current_time + number
        when '-'
          return current_time - number
        end
      end
      Time.iso8601(diff_str)
    end

    def _compare_times(time_a, operator, time_b)
      case operator
      when '>'
        return time_a.nil? || (time_a <=> time_b) > 0
      when '<'
        return time_a.nil? || (time_a <=> time_b) < 0
      when '>='
        return time_a.nil? || (time_a <=> time_b) >= 0
      when '<='
        return time_a.nil? || (time_a <=> time_b) <= 0
      when '=='
        return time_a.nil? || (time_a <=> time_b) == 0
      when '!='
        return time_a.nil? || (time_a <=> time_b) != 0
      else
        fail 'invalid comparison operator'
      end
      false
    end

    def _regexp_format(format:, deny_no_name: false)
      regex = nil
      unless format.start_with?('/') && format.end_with?('/')
        fail "Invalid regexp in format '#{format[1..-2]}'"
      end
      begin
        regex = @multiline ? Regexp.new(format[1..-2], Regexp::MULTILINE) : Regexp.new(format[1..-2])
      rescue => e
        raise "Invalid regexp in format '#{format[1..-2]}': #{e.message}"
      end
      fail "No named captures in format '#{format[1..-2]}'" if deny_no_name && regex.named_captures.empty?
      regex
    end

    def _iam_role?
      ec2 = Aws::EC2::Client.new(region: @region)
      s3 = Aws::S3::Client.new(region: @region)
      !ec2.config.credentials.nil? && !s3.config.credentials.nil?
    rescue => e
      raise "Aws Client error occurred: #{e.message}"
    end

    def _add_end_slash(str)
      str.end_with?('/') ? str : str + '/'
    end

    def start
      super

      #@db = Datastore.instance
      @db = Datastore.new
      @db.connect(work_dir: @work_dir, clear: @clear_db_at_start)

      _clear_queues
      @timer = Thread.new(&method(:_timer))
      @thread = Thread.start do
        while @start_queue.pop
          break if @shutdown_flag
          @status = Status::RUNNING
          run
          @status = Status::WAITING
        end
        @status = Status::SHUTDOWN
      end
      @thread
    end

    def _timer
      loop do
        sleep @refresh_interval unless @start_now
        @start_now = false if @start_now
        @start_queue.push true
      end
    end

    def shutdown
      super
      @timer.kill
      @shutdown_flag = true
      @start_queue.push nil
      @parse_thread_num.times { @archive_parse_queue.push nil }
      @parse_thread_num.times { @current_parse_queue.push nil }
      @download_thread_num.times { @download_queue.push nil }
      # thread timeout 30sec
      @thread.join 30
      _clear_queues
      @db.close
    end

    def _clear_queues
      @start_queue.clear
      @download_queue.clear
      @current_parse_queue.clear
      @archive_parse_queue.clear
    end

    def run
      obj_list = _diff_objects
      return if obj_list.size <= 0
      obj_list.each { |obj| @download_queue.push obj }
      threads = []
      parse_finish_queue = Queue.new
      dl_finish_queue = Queue.new
      @download_thread_num.times do
        threads << Thread.new do
          while obj = @download_queue.pop
            break if @shutdown_flag
            _download_object obj
            break if @shutdown_flag
            @archive_parse_queue.push obj
            dl_finish_queue.push true
            if @download_queue.empty?
              @download_thread_num.times { @download_queue.push nil }
            end
          end
        end
      end
      @parse_thread_num.times do
        threads << Thread.new do
          while obj = @archive_parse_queue.pop
            break if @shutdown_flag
            if obj[:current]
              @current_parse_queue.push obj
            else
              _parse_object obj
            end
            if @archive_parse_queue.empty? && dl_finish_queue.size == obj_list.size
              @parse_thread_num.times { @archive_parse_queue.push nil }
            end
          end
          sleep 0.01 until @archive_parse_queue.empty?
          @current_parse_queue.push nil if @current_parse_queue.empty?
          while obj = @current_parse_queue.pop
            break if @shutdown_flag
            _parse_object obj
            @current_parse_queue.push nil if @current_parse_queue.empty?
          end
        end
      end
      threads.each(&:join)
    rescue => e
      raise "error occurred: #{e.message}, #{e.backtrace.join("\n")}"
    end

    def _diff_objects
      s3_obj_list = _s3_objects(prefix: @s3_prefix)
      diff = []
      s3_obj_list.each do |obj|
        record = @db.init_record(obj)
        diff << record unless record.nil?
      end
      diff
    end

    def _s3_objects(prefix:, objects: [], next_marker: nil, timestamp: Time.now.utc)
      return objects if @shutdown_flag
      resp = _s3_client.list_objects(
        bucket: @s3_bucket,
        delimiter: '/',
        marker: next_marker,
        prefix: prefix
      )
      resp.contents.each do |obj|
        next if obj.key.end_with?('/') || obj.size <= 0

        key_match = nil
        unless @s3_key_format_regexp.nil?
          key_match = @s3_key_format_regexp.match(obj.key)
          next if key_match.nil?
        end

        unless @s3_key_exclude_format_regexp.nil?
          exkey_match = @s3_key_exclude_format_regexp.match(obj.key)
          next unless exkey_match.nil?
        end

        skip_flag = false
        key_match.names.each do |name|
          next unless @date_conditions.collect { |item| item[:group_name] }.include?(name)
          @date_conditions.each do |condition|
            unless _validate_date_condition(time_str: key_match[name.to_sym], condition_str: condition[:condition_str], current_time: timestamp)
              skip_flag = true
              break
            end
          end
          break if skip_flag
        end
        next if skip_flag

        objects << @db.default_schema.merge(
          {
            bucket: resp.name,
            key: obj.key,
            size: obj.size.to_i,
            modified: obj.last_modified.iso8601,
            current: _current_target?(obj.key),
            # TODO REMOVE
            #position: 0
          }
        )
      end
      _s3_objects(
        prefix: prefix, objects: objects, next_marker: resp.next_marker, timestamp: timestamp
      ) if resp.is_truncated
      resp.common_prefixes.each do |cmn_prefix|
        _s3_objects(prefix: cmn_prefix.prefix, objects: objects, timestamp: timestamp)
      end if next_marker.nil?
      objects
    end

    def _current_target?(key)
      return false if @s3_key_current_format_regexp.nil?
      !@s3_key_current_format_regexp.match(key).nil?
    end

    def _download_object(obj)
      file = _file_path(bucket: obj[:bucket], key: obj[:key])
      begin
        File.delete file if FileTest.exist? file
        resp = _s3_client.get_object(
          response_target: (/\.gz$/ =~ obj[:key]) ? file + '.gz' : file,
          bucket: obj[:bucket],
          key: obj[:key]
        )
        obj[:size] = resp.content_length
        obj[:modified] = resp.last_modified.iso8601
        return FileTest.exist? file
      rescue => e
        $log.error "S3 GetObject error occurred: #{e.message}"
        File.delete file if FileTest.exist? file
        return false
      end
    end

    def _file_path(bucket:, key:)
      @work_dir + Digest::MD5.hexdigest("#{bucket}/#{key}")
    end

    def _s3_client
      options = { region: @region }
      options.merge!(
        access_key_id: @access_key_id,
        secret_access_key: @secret_access_key
      ) if @access_key_id && @secret_access_key
      Aws::S3::Client.new(options)
    rescue => e
      raise "S3 Client error occurred: #{e.message}"
    end

    def _ends_line(file, position)
      first_line = nil
      last_line = nil
      File.open(file, File::RDONLY) do |f|
        first_line = f.gets
        if f.pos >= position
          last_line = first_line
          break
        end
        cr_count = 0
        f.seek(position - 1, IO::SEEK_SET)
        while char = f.read(1)
          if char == "\n"
            break if cr_count > 0
            cr_count += 1
          end
          f.seek(f.pos - 2, IO::SEEK_SET)
        end
        last_line = f.gets
      end
      [first_line, last_line]
    end

    def _parse_object(obj)
      file = _file_path(bucket: obj[:bucket], key: obj[:key])

      if /\.gz$/ =~ obj[:key]
        Zlib::GzipReader.open(file + '.gz', encoding: 'UTF-8') { |f| File.write(file, f.read) }
        File.delete file + '.gz'
      end

      first_line, last_line = _ends_line(file, obj[:position])
      obj[:first_line] = first_line

      return if @shutdown_flag

      File.open(file, File::RDONLY) { |f| _read_object_lines f, obj }

      File.delete file
    rescue => e
      $log.error "error occurred: #{e.message} #{e.backtrace}"
    end

    def _read_object_lines(f, obj)
      return if @shutdown_flag
      current_record = @db.search_current_record obj
      line_buf = []
      instance_tag_cache = {}
      f.seek(obj[:position], IO::SEEK_SET)
      while line = f.gets
        break if @shutdown_flag
        line_buf = [] if @multiline

        unless @format_firstline_regexp.nil?
          firstline_match = @format_firstline_regexp.match(line)
          line_buf = [] unless firstline_match.nil?
        end

        line_buf << line
        line_match = @format_regexp.match(line_buf.join(''))
        next if line_match.nil?

        line_buf = []
        emit_record = {}
        log_time = nil
        line_match.names.each do |name|
          emit_record[name] = line_match[name.to_sym]

          if !@timestamp.empty? && name == @timestamp[:group_name] && !emit_record[name].nil?
            begin
              log_time = _strptime_with_timezone(emit_record[name], @timestamp[:format])
            rescue => e
              $log.warn "error occurred: #{e.message}"
              log_time = Fluent::Engine.now
            end
          end

          next if !@add_instance_tags || name != 'instance_id' || emit_record[name].nil?
          instance_id = line_match[name.to_sym]
          if instance_tag_cache[instance_id].nil?
            instance_tag_cache[instance_id] = _describe_instance_tags(instance_id)
          end
          instance_tag_cache[instance_id].each do |key, value|
            emit_record[key] = value
          end
        end

        obj[:position] = f.pos
        obj[:last_line] = line

        @db.transaction do |db|
          unless current_record.nil?
            db.where(id: current_record[:id]).update(
              size: 0,
              position: 0,
              first_line: nil,
              last_line: nil
            )
            current_record = nil
          end
          @timestamp.empty?
          @router.emit(@tag, log_time, emit_record)
          db.where(id: obj[:id]).limit(1).update(obj)
          @emit_counter += 1
        end

        break if @shutdown_flag
      end
    end

    def _describe_instance_tags(instance_id)
      tags = _ec2_client.describe_instances(instance_ids: [instance_id]).reservations[0].instances[0].tags
      new_tag_hash = {}
      tags.each { |tag| new_tag_hash.store(tag[:key], tag[:value]) }
      new_tag_hash
    rescue => e
      raise "EC2 Client error occurred: #{e.message}"
    end

    def _ec2_client
      options = { region: @region }
      options.merge!(
        access_key_id: @access_key_id,
        secret_access_key: @secret_access_key
      ) if @access_key_id && @secret_access_key
      Aws::EC2::Client.new(options)
    rescue => e
      raise "EC2 Client error occurred: #{e.message}"
    end

    class Datastore
      #include Singleton

      attr_accessor :counter

      def initialize
        @semaphore = Mutex.new
      end

      def connect(work_dir:, clear: false)
        db = nil
        @work_dir = work_dir
        db = Sequel.sqlite(@work_dir + 's3in.sqlite')
        #db = Sequel.sqlite

        # http://arbitrage.jpn.org/it/2015-07-07-2/
        {
          'journal_mode' => 'MEMORY',
          #'journal_mode' => 'wal',
          #'journal_mode' => 'Persist',
          'synchronous' => 'OFF',
          'busy_timeout' => 50_000
        }.each { |key, value| db.pragma_set(key, value) }

        db.drop_table(:objects) if clear && db.table_exists?(:objects)
        db.create_table :objects do
          primary_key :id
          String  :bucket,          allow_null: false, index: true
          String  :key,             allow_null: false, index: true
          Integer :size,            allow_null: false, index: false, default: 0
          Boolean :current,         allow_null: false, index: true, default: false
          Integer :position,        allow_null: false, index: false, default: 0
          String :modified,         allow_null: false, index: false
          String  :first_line,      allow_null: true,  index: true, default: nil
          String  :last_line,       allow_null: true,  index: true, default: nil
        end unless db.table_exists?(:objects)
        @db = db
      end

      def close
        @db.disconnect
        Sequel::DATABASES.delete(@db)
      end

      def default_schema
        if @schema_hash.nil?
          @schema_hash = {}
          @db.schema(:objects).collect { |item| @schema_hash[item[0]] = item[1][:default] }
          @schema_hash[:size] = @schema_hash[:size].to_i
          @schema_hash[:position] = @schema_hash[:position].to_i
        end
        @schema_hash.clone
      end

      def transaction
        @semaphore.synchronize do
          @db.transaction { yield @db[:objects] }
        end
      end

      def init_record(obj)
        transaction do |db|
          record = db.first(bucket: obj[:bucket], key: obj[:key])
          if record.nil?
            obj[:id] = db.insert(obj)
          else
            return nil if Time.iso8601(obj[:modified]) == Time.iso8601(record[:modified])
            obj[:id] = record[:id]
            obj[:position] = record[:position]
            obj[:first_line] = record[:first_line]
            obj[:last_line] = record[:last_line]
          end
          obj
        end
      end

      def search_current_record(obj)
        return nil unless obj[:current]
        transaction do |db|
          return db.exclude(
            bucket: obj[:bucket],
            key: obj[:key]
          ).where(
            current: true,
            first_line: obj[:first_line],
            last_line: obj[:last_line]
          ).first
        end
      end
    end
  end
end
