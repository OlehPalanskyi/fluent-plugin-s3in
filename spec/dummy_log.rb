require 'apache-loggen/base'
require 'zlib'

class DummyLog
  define_method(:files) { @results }

  attr_reader :total_record_num

  def initialize(work_dir)
    @work_dir = work_dir
    @results = []
    @total_record_num = 0
  end

  def generate(record_num: 1000, file_num: 1, file_name: nil, gz: false)
    @total_record_num += record_num * file_num
    file_num.times do |num|
      file = @work_dir + '/' + (file_name.nil? ? Time.now.strftime('%Y-%m-%d') + '.' + num.to_s + '.log' : file_name)
      LogGenerator.generate(
        {
          limit: record_num - 1,
          filename: file
        }, MyGen.new
      )
      if gz
        Zlib::GzipWriter.open(file + '.gz') { |f| f.write File.read(file) }
        File.delete(file)
        file += '.gz'
      end
      @results.push(file: file, record_num: record_num)
    end
    @results
  end

  def delete
    @results.each { |obj| File.delete(obj[:file]) if File.exist?(obj[:file]) }
    @results = []
  end

  class MyGen < LogGenerator::Apache
    def format(record, config)
      record['uuid'] = SecureRandom.uuid
      if config[:json]
        return record.to_json + "\n"
      else
        return %[#{record['host']} - #{record['user']} [#{Time.now.strftime('%d/%b/%Y:%H:%M:%S %z')}] "#{record['method']} #{record['path']} HTTP/1.1" #{record['code']} #{record['size']} "#{record['referer']}" "#{record['agent']}" #{record['uuid']}\n]
      end
    end
  end
end
