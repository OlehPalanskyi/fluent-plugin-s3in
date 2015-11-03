require 'helper'
require 'logger'
require 'tmpdir'
require 'glint'
require 'pp'

class S3InputTest < Test::Unit::TestCase

  DEFAULT_CONFIG = {
    region: 'ap-northeast-1',
    access_key_id: nil,
    secret_access_key: nil,
    s3_bucket: 'dummy_bucket',
    s3_prefix: 'dummy_prefix',
    s3_key_format: nil,
    s3_key_exclude_format: nil,
    s3_key_current_format: nil,
    format: '/(?<log>.+)/',
    format_firstline: nil,
    work_dir: './temp/',
    timestamp: nil,
    timezone: 'UTC',
    db_name: 's3in',
    db_type: 'sqlite',
    clear_db_at_start: false,
    interval: 300,
    start_now: true,
    download_thread_num: 5,
    parse_thread_num: 5,
    add_instance_tags: true
  }

  class << self
    def startup
      rootdir = Dir.mktmpdir
      server = Glint::Server.new(nil, signals: [:INT]) do |port|
        exec "bundle exec fakes3 -p #{port} -r #{rootdir}", err: '/dev/null'
        exit 0
      end
      server.start

      Glint::Server.info[:fakes3] = {
        address: "127.0.0.1:#{server.port}",
        root: rootdir
      }
    end

    def shutdown
      if Dir.exists? Glint::Server.info[:fakes3][:root]
        FileUtils.remove_entry_secure(Glint::Server.info[:fakes3][:root])
      end
    end
  end

  def setup
    Fluent::Test.setup
    $log = Logger.new STDOUT
  end

  def parse_config(conf = {})
    ''.tap { |s| conf.each { |k, v| s << "#{k} #{v.to_s}\n" unless v.nil? } }
  end

  def create_driver(conf = DEFAULT_CONFIG)
    Fluent::Test::InputTestDriver.new(Fluent::S3Input).configure(parse_config conf)
  end

  sub_test_case '#configure' do
    test 'config_paramが正常値の場合に例外が発生しないこと' do
      create_driver
    end

    test 'access_key_id と secret_access_key が両方指定されない場合に例外が発生すること' do
      conf = {
        access_key_id: 'dummy',
        secret_access_key: nil
      }
      assert_raise(Fluent::ConfigError) do
        create_driver DEFAULT_CONFIG.merge(conf)
      end
      conf = {
        access_key_id: nil,
        secret_access_key: 'dummy'
      }
      assert_raise(Fluent::ConfigError) do
        create_driver DEFAULT_CONFIG.merge(conf)
      end
      conf = {
        access_key_id: nil,
        secret_access_key: nil
      }
      assert_nothing_raised(Fluent::ConfigError) do
        create_driver DEFAULT_CONFIG.merge(conf)
      end
      conf = {
        access_key_id: 'dummy',
        secret_access_key: 'dummy'
      }
      assert_nothing_raised(Fluent::ConfigError) do
        create_driver DEFAULT_CONFIG.merge(conf)
      end
    end
  end

  # sub_test_case '#diff_objects' do
  #   test 'diff_objects' do
  #     conf = {
  #       s3_bucket: 'testeutfjhyrdtgf',
  #       s3_prefix: 'AWSLogs/486333402723/CloudTrail/ap-northeast-1/temp/2015/01/15/'
  #     }
  #     s3input = create_driver(DEFAULT_CONFIG.merge(conf)).instance
  #     s3input.diff_objects
  #   end
  # end

  sub_test_case '#run' do
    def setup
      # FileUtils.rm_r DEFAULT_CONFIG[:work_dir] if Dir.exist? DEFAULT_CONFIG[:work_dir]
      # FileUtils.mkdir_p DEFAULT_CONFIG[:work_dir]
    end

    def teardown
    end

    test 'run_with_sqlite_memory' do
      conf = {
        s3_bucket: 'testeutfjhyrdtgf',
        s3_prefix: 'test3',
        #s3_key_format: '/(?<date>[0-9]{4}[0-9]{2})\/dmy[0-9]+\.log/',
        s3_key_format: '/dmy1.log.gz/',
        db_name: 's3in',
        db_type: 'sqlite',
        clear_db_at_start: true,
        timestamp: 'time',
        work_dir: './temp/',
        #s3_key_exclude_format: '/dmy1/',
        #s3_key_current_format: '/dmy1/',
        #s3_key_date_condition1: 'date %Y%m > -5356800',

        #s3_prefix: 'test3',
        #s3_key_format: '/dmy1\.log/',
        format: '/^(?<date>[^ ]+) (?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] "(?<method>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*) [^ ]* "(?<agent>[^\"]*)"(?<instance_id>i-.*)?$/'
      }
      conf2 = {
        s3_bucket: 'testeutfjhyrdtgf',
        s3_prefix: 'test2',
        s3_key_format: '/(?<date>[0-9]{4}[0-9]{2})\/dmy[0-9]+\.log/',
        s3_key_exclude_format: '/dmy1/',
        db_name: 's3in',
        db_type: 'sqlite',
        clear_db_at_start: false,
        timestamp: 'time %Y-%m',
        work_dir: './temp2/',
        format: '/^(?<date>[^ ]+) (?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] "(?<method>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*) [^ ]* "(?<agent>[^\"]*)"$/'
      }

      # d = create_driver(DEFAULT_CONFIG.merge(conf))
      # d.run { sleep 2 }
      # @watch.lap
      # emits = d.emits
      # count += emits.length

      FileUtils.rm_r conf[:work_dir] if Dir.exist? conf[:work_dir]
      FileUtils.mkdir_p conf[:work_dir]
      FileUtils.rm_r conf2[:work_dir] if Dir.exist? conf2[:work_dir]
      FileUtils.mkdir_p conf2[:work_dir]
      threads = []
      threads << Thread.new do
        watch = Stopwatch.new('run_with_sqlite_memory1')
        count = 0
        d = create_driver(DEFAULT_CONFIG.merge(conf))
        d.run { sleep 0.5 until d.instance.status == Fluent::S3Input::Status::WAITING }
        watch.lap
        puts watch.log
        p d.emits.length
        p d.instance.emit_counter
      end
      # threads << Thread.new do
      #   #sleep 1
      #   watch = Stopwatch.new('run_with_sqlite_memory2')
      #   count = 0
      #   d2 = create_driver(DEFAULT_CONFIG.merge(conf2))
      #   d2.run { sleep 0.5 until d2.instance.status == Fluent::S3Input::Status::WAITING }
      #   watch.lap
      #   puts watch.log
      #   p d2.emits.length
      #   p d2.instance.emit_counter
      # end
      threads.each(&:join)

      # d = create_driver(DEFAULT_CONFIG.merge(conf))
      # d.run { sleep 1 }
      # @watch.lap
      # emits = d.emits
      # count += emits.length

    end

    # test 'run_with_sqlite_file' do
    #   @watch.title = 'run_with_sqlite_file'
    #   conf = {
    #     s3_bucket: 'testeutfjhyrdtgf',
    #     s3_prefix: 'test2',
    #     s3_key_format: '/(?<date>[0-9]{4}[0-9]{2})\/dmy[0-9]+\.log/',
    #     db_type: 'sqlite',
    #     clear_db_at_start: true,
    #     format: '/^(?<date>[^ ]+) (?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] "(?<method>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*) [^ ]* "(?<agent>[^\"]*)"$/'
    #   }
    #   count = 0

    #   d = create_driver(DEFAULT_CONFIG.merge(conf))
    #   d.run { sleep 0.5 until d.instance.status == Fluent::S3Input::Status::WAITING }
    #   @watch.lap
    #   emits = d.emits
    #   count += emits.length

    #   puts count
    # end

    # test 'run_with_mysql' do
    #   @watch.title = 'run_with_mysql'
    #   conf = {
    #     s3_bucket: 'testeutfjhyrdtgf',
    #     s3_prefix: 'test2',
    #     s3_key_format: '/(?<date>[0-9]{4}[0-9]{2})\/dmy[0-9]+\.log/',
    #     db_name: 's3in',
    #     db_type: 'mysql',
    #     clear_db_at_start: true,
    #     format: '/^(?<date>[^ ]+) (?<host>[^ ]*) [^ ]* (?<user>[^ ]*) \[(?<time>[^\]]*)\] "(?<method>\S+)(?: +(?<path>[^ ]*) +\S*)?" (?<code>[^ ]*) (?<size>[^ ]*) [^ ]* "(?<agent>[^\"]*)"$/'
    #   }
    #   count = 0

    #   d = create_driver(DEFAULT_CONFIG.merge(conf))
    #   d.run { sleep 0.5 until d.instance.status == Fluent::S3Input::Status::WAITING }
    #   @watch.lap
    #   emits = d.emits
    #   count += emits.length

    #   puts count
    # end
  end



  # sub_test_case '#configure2' do
  #   test 'config_paramが正常値の場合に例外が発生しないこと' do
  #     @region = 'ap-northeast-1'
  #     tags =  _describe_instance_tags 'i-8bbb1b2e'
  #     pp tags
  #   end
  # end

  # def _describe_instance_tags(instance_id)
  #   tags = _ec2_client.describe_instances(instance_ids: [instance_id]).reservations[0].instances[0].tags
  #   new_tag_hash = {}
  #   tags.each { |tag| new_tag_hash.store(tag[:key], tag[:value]) }
  #   new_tag_hash
  # rescue => e
  #   raise "EC2 Client error occurred: #{e.message}"
  # end

  # def _ec2_client
  #   options = { region: @region }
  #   options.merge!(
  #     access_key_id: @access_key_id,
  #     secret_access_key: @secret_access_key
  #   ) if @access_key_id && @secret_access_key
  #   Aws::EC2::Client.new(options)
  # rescue => e
  #   raise "EC2 Client error occurred: #{e.message}"
  # end

  # def datastore_clear(db_name: DEFAULT_CONFIG[:db_name])
  #   db = Sequel.sqlite(DEFAULT_CONFIG[:work_dir] + db_name)
  #   db.create_table :objects do
  #     primary_key :id
  #     String :bucket
  #     String :key, unique: true, index: true
  #     String :etag
  #     Integer :size
  #     String :last_modified
  #     Integer :position, default: 0
  #   end unless db.table_exists?(:objects)
  #   db[:objects].delete
  # end
  class Stopwatch

    attr_accessor :title

    def initialize(title = 'test')
      @title = title
      @start = Time.now
      @lap_times = []
      @messages = []
      @messages << 'Started: ' + @start.to_s
    end

    def lap
      now = Time.now
      @messages << 'Lap' + @messages.size.to_s + ': ' + now.to_s + ' ( Elapsed time: ' + (now - @start).to_s + 's )'
      log
    end

    def log
      "\n--- " + @title + " ---\n" + @messages.join("\n") + "\n"
    end
  end
end
