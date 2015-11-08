require 'logger'
require 'timeout'
require_relative 'spec_helper'
require_relative 'fakes3_server'
require_relative 'dummy_log'

WORK_DIR = File.expand_path('../../temp/work', __FILE__)
S3_SERVER_DIR = File.expand_path('../../temp/s3', __FILE__)
DUMMY_LOG_TEMP_DIR = File.expand_path('../../temp/dmy_log', __FILE__)

describe Fluent::S3Input do
  before(:all) do
    FileUtils.rm_r WORK_DIR if Dir.exist? WORK_DIR
    FileUtils.mkdir_p WORK_DIR

    FileUtils.rm_r S3_SERVER_DIR if Dir.exist? S3_SERVER_DIR
    FileUtils.mkdir_p S3_SERVER_DIR

    FileUtils.rm_r DUMMY_LOG_TEMP_DIR if Dir.exist? DUMMY_LOG_TEMP_DIR
    FileUtils.mkdir_p DUMMY_LOG_TEMP_DIR

    Fluent::Test.setup
    FakeS3Server.instance.start(dir: S3_SERVER_DIR)
  end

  after(:all) do
    FakeS3Server.instance.shutdown(delete_dir: false)
  end

  let(:region) { 'ap-northeast-1' }

  let(:enable_iam_role) do
    dummy_cred = (Class.new { define_method(:credentials) { false } }).new

    ec2_client_mock = double('Aws EC2 Client')
    allow(ec2_client_mock).to receive(:config).and_return(dummy_cred)
    allow(Aws::S3::Client).to receive(:new).with(region: region).and_return(ec2_client_mock)

    s3_client_mock = double('Aws S3 Client')
    allow(s3_client_mock).to receive(:config).and_return(dummy_cred)
    allow(Aws::EC2::Client).to receive(:new).with(region: region).and_return(s3_client_mock)
  end

  let(:disable_iam_role) do
    dummy_cred = (Class.new { define_method(:credentials) { nil } }).new

    ec2_client_mock = double('Aws EC2 Client')
    allow(ec2_client_mock).to receive(:config).and_return(dummy_cred)
    allow(Aws::S3::Client).to receive(:new).with(region: region).and_return(ec2_client_mock)

    s3_client_mock = double('Aws S3 Client')
    allow(s3_client_mock).to receive(:config).and_return(dummy_cred)
    allow(Aws::EC2::Client).to receive(:new).with(region: region).and_return(s3_client_mock)
  end

  let(:enable_region_mock) do
    uri = URI.parse('http://localhost/dummy')
    http = Net::HTTP.new(uri.host, uri.port)
    allow(http).to receive(:get).and_return(region + 'a')
    allow(Net::HTTP).to receive(:new).and_return(http)
  end

  let(:default_conf) do
    {
      s3_bucket: 'dummy_bucket',
      s3_prefix: 'dummy_prefix',
      format: '/(?<log>.+)/',
      work_dir: WORK_DIR
    }
  end

  let(:test_driver) { Fluent::Test::InputTestDriver.new(Fluent::S3Input) }

  let(:remake_work_dir) do
    FileUtils.rm_r WORK_DIR if Dir.exist? WORK_DIR
    FileUtils.mkdir_p WORK_DIR
  end

  def create_driver(conf)
    Fluent::Test::InputTestDriver.new(Fluent::S3Input).configure(conf)
  end

  def parse_conf(hash)
    ''.tap { |s| hash.each { |k, v| s << "#{k} #{v.to_s}\n" unless v.nil? } }
  end

  def validate_records(records, target_files)
    target_lines = []
    target_files.each do |f|
      target_lines.concat(File.readlines(f))
    end
    records.each { |record| fail unless target_lines.include?(record + "\n") }
    true
  rescue
    false
  end

  describe '#configure' do
    context '必須パラメータ検証' do
      let(:conf) do
        {
          s3_bucket: 'dummy_bucket',
          s3_prefix: 'dummy_prefix',
          format: '/(?<log>.+)/',
          work_dir: './temp/'
        }
      end

      before(:each) do
        enable_region_mock
        enable_iam_role
      end

      it 'format が指定されない場合に例外が発生すること' do
        conf.delete(:format)
        expect { test_driver.configure(parse_conf conf) }.to raise_error(Fluent::ConfigError)
      end

      it 'work_dir が指定されない場合に例外が発生すること' do
        conf.delete(:work_dir)
        expect { test_driver.configure(parse_conf conf) }.to raise_error(Fluent::ConfigError)
      end

      it 's3_bucket が指定されない場合に例外が発生すること' do
        conf.delete(:s3_bucket)
        expect { test_driver.configure(parse_conf conf) }.to raise_error(Fluent::ConfigError)
      end

      it 's3_prefix が指定されない場合に例外が発生すること' do
        conf.delete(:s3_prefix)
        expect { test_driver.configure(parse_conf conf) }.to raise_error(Fluent::ConfigError)
      end
    end

    context 'AWS regionパラメータ検証' do
      before(:each) { enable_iam_role }

      it 'region が指定されない かつ regionが取得できない場合に例外が発生すること' do
        uri = URI.parse('http://localhost/dummy')
        http = Net::HTTP.new(uri.host, uri.port)
        allow(http).to receive(:get).and_return(nil)
        allow(Net::HTTP).to receive(:new).and_return(http)
        expect { test_driver.configure(parse_conf default_conf) }.to raise_error(Fluent::ConfigError)
      end

      it 'region が指定されない かつ regionが取得できる場合に例外が発生しないこと' do
        enable_region_mock
        expect { test_driver.configure(parse_conf default_conf) }.not_to raise_error
      end

      it 'region が指定された場合に regionを取得しない かつ 例外が発生しないこと' do
        conf = { region: 'ap-northeast-1' }.merge(default_conf)
        expect { test_driver.configure(parse_conf conf) }.not_to raise_error
      end
    end

    context 'AWS 証明書パラメータ検証' do
      before(:each) { enable_region_mock }

      it 'IAMRole が無い かつ access_key_id と secret_access_key が両方指定された場合に例外が発生しないこと' do
        disable_iam_role
        conf = {
          access_key_id: 'dummy',
          secret_access_key: 'dummy'
        }.merge(default_conf)
        expect { test_driver.configure(parse_conf conf) }.not_to raise_error
      end

      it 'IAMRole が有る かつ access_key_id と secret_access_key が両方指定された場合に例外が発生しないこと' do
        enable_iam_role
        conf = {
          access_key_id: 'dummy',
          secret_access_key: 'dummy'
        }.merge(default_conf)
        expect { test_driver.configure(parse_conf conf) }.not_to raise_error
      end

      it 'IAMRole が有る かつ access_key_id と secret_access_key が両方指定されない場合に例外が発生しないこと' do
        enable_iam_role
        expect { test_driver.configure(parse_conf default_conf) }.not_to raise_error
      end

      it 'IAMRole が無い かつ access_key_id と secret_access_key が両方指定されない場合に例外が発生すること' do
        disable_iam_role
        expect { test_driver.configure(parse_conf default_conf) }.to raise_error(Fluent::ConfigError)
      end

      it 'IAMRole が無い かつ access_key_id が指定される かつ secret_access_key が指定されない場合に例外が発生すること' do
        disable_iam_role
        conf = { access_key_id: 'dummy' }.merge(default_conf)
        expect { test_driver.configure(parse_conf conf) }.to raise_error(Fluent::ConfigError)
      end

      it 'IAMRole が無い かつ access_key_id が指定されない かつ secret_access_key が指定される場合に例外が発生すること' do
        disable_iam_role
        conf = { secret_access_key: 'dummy' }.merge(default_conf)
        expect { test_driver.configure(parse_conf conf) }.to raise_error(Fluent::ConfigError)
      end

      it 'IAMRole が有る かつ access_key_id が指定される かつ secret_access_key が指定されない場合に例外が発生すること' do
        enable_iam_role
        conf = { access_key_id: 'dummy' }.merge(default_conf)
        expect { test_driver.configure(parse_conf conf) }.to raise_error(Fluent::ConfigError)
      end

      it 'IAMRole が有る かつ access_key_id が指定されない かつ secret_access_key が指定される場合に例外が発生すること' do
        enable_iam_role
        conf = { secret_access_key: 'dummy' }.merge(default_conf)
        expect { test_driver.configure(parse_conf conf) }.to raise_error(Fluent::ConfigError)
      end
    end
  end

  describe '#start' do
    before(:each) do
      # enable_region_mock
      # enable_iam_role
    end

    let(:s3_client) do
      Aws::S3::Client.new(
        region: region,
        access_key_id: 'DUMMY_ACCESS_KEY_ID',
        secret_access_key: 'DUMMY_SECRET_ACCESS_KEY',
        endpoint: FakeS3Server.instance.endpoint,
        force_path_style: true)
    end
    let(:enable_s3_client_mock) do
      s3_client = Aws::S3::Client.new(
        region: region,
        access_key_id: 'DUMMY_ACCESS_KEY_ID',
        secret_access_key: 'DUMMY_SECRET_ACCESS_KEY',
        endpoint: FakeS3Server.instance.endpoint,
        force_path_style: true)
      allow(Aws::S3::Client).to receive(:new).and_return(s3_client)
    end
    let(:dummy_log) { DummyLog.new(DUMMY_LOG_TEMP_DIR) }

    context '圧縮形式ファイル' do
    end

    context 'Text形式ファイル' do
      before(:each) do
        enable_s3_client_mock
        s3_client.create_bucket(bucket: 'dummy_bucket')
      end

      after(:each) do
        s3_client.list_objects(bucket: 'dummy_bucket').contents.each do |obj|
          s3_client.delete_object(bucket: 'dummy_bucket', key: obj.key)
        end
        s3_client.delete_bucket(bucket: 'dummy_bucket')
        dummy_log.delete
      end

      let(:conf) do
        {
          region: 'ap-northeast-1',
          access_key_id: 'dummy',
          secret_access_key: 'dummy',
          s3_bucket: 'dummy_bucket',
          s3_prefix: 'dummy_prefix',
          format: '/(?<log>.+)/',
          work_dir: WORK_DIR,
          clear_db_at_start: true,
          start_now: true
        }
      end

      it '追記したレコードの合計数と、emitされたレコード数が一致すること' do
        step_queues = [Queue.new, Queue.new]
        threads = []

        dummy_log.generate(record_num: 67, file_name: 'app.current.log')
        key = conf[:s3_prefix] + '/' + File.basename(dummy_log.files[0][:file])
        s3_client.put_object(bucket: conf[:s3_bucket], key: key, body: File.open(dummy_log.files[0][:file]))

        threads << Thread.new do
          step_queues[0].pop
          dummy_log.generate(record_num: 33, file_name: 'app.current.log')
          s3_client.delete_object(bucket: conf[:s3_bucket], key: key)
          s3_client.put_object(bucket: conf[:s3_bucket], key: key, body: File.open(dummy_log.files[0][:file]))
          step_queues[1].push(true)
        end

        d = test_driver.configure(parse_conf conf)
        threads << Thread.new do
          d.run do
            sleep 0.1 until d.instance.status == Fluent::S3Input::Status::WAITING
            step_queues[0].push(true)
            step_queues[1].pop
            d.instance.start_queue.push(true)
            sleep 1
            sleep 0.1 until d.instance.status == Fluent::S3Input::Status::WAITING
          end
        end
        threads.each(&:join)

        emits = d.emits

        expect(dummy_log.total_record_num).to eq(100)
        expect(emits.length).to eq(dummy_log.total_record_num)
        expect(
          validate_records(
            emits.collect { |obj| obj[2]['log'] }, dummy_log.files.collect { |obj| obj[:file] }
          )
        ).to eq(true)
      end

      it 'ファイルのレコード数と、emitしたレコード数が一致すること' do
        dummy_log.generate(record_num: 100, file_num: 2)
        dummy_log.files.each do |obj|
          key = conf[:s3_prefix] + '/' + File.basename(obj[:file])
          s3_client.put_object(bucket: conf[:s3_bucket], key: key, body: File.open(obj[:file]))
        end

        d = test_driver.configure(parse_conf conf)
        d.run { sleep 0.5 until d.instance.status == Fluent::S3Input::Status::WAITING }
        emits = d.emits

        expect(emits.length).to eq(dummy_log.total_record_num)
        expect(
          validate_records(
            emits.collect { |obj| obj[2]['log'] }, dummy_log.files.collect { |obj| obj[:file] }
          )
        ).to eq(true)
      end

      it '1000レコード×10ファイルのemitが10秒以内で終了すること' do
        dummy_log.generate(record_num: 1000, file_num: 10)
        dummy_log.files.each do |obj|
          key = conf[:s3_prefix] + '/' + File.basename(obj[:file])
          s3_client.put_object(bucket: conf[:s3_bucket], key: key, body: File.open(obj[:file]))
        end

        d = test_driver.configure(parse_conf conf)

        begin
          timeout(10) do
            d.run { sleep 0.5 until d.instance.status == Fluent::S3Input::Status::WAITING }
          end
        rescue Timeout::Error
          raise
        end

        emits = d.emits
        expect(emits.length).to eq(dummy_log.total_record_num)
      end
    end
  end

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
