require_relative 'spec_helper'

require 'tmpdir'
require 'glint'
require 'pp'

WORK_DIR = File.expand_path('../../temp', __FILE__)

describe Fluent::S3Input do
  let(:region) { 'ap-northeast-1' }
  let(:default_conf) do
    %[
      s3_bucket dummy_bucket
      s3_prefix dummy_prefix
      format /(?<log>.+)/
      work_dir #{WORK_DIR}
    ]
  end
  let(:test_driver) { Fluent::Test::InputTestDriver.new(Fluent::S3Input) }

  before(:all) do
    Fluent::Test.setup

    FileUtils.rm_r WORK_DIR if Dir.exist? WORK_DIR
    FileUtils.mkdir_p WORK_DIR
  end

  after(:all) do
    FileUtils.rm_r WORK_DIR
  end

  def create_driver(conf)
    Fluent::Test::InputTestDriver.new(Fluent::S3Input).configure(conf)
  end

  def hash_to_conf(hash)
    ''.tap { |s| hash.each { |k, v| s << "#{k} #{v.to_s}\n" unless v.nil? } }
  end

  describe '#configure' do
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

    context '必須パラメータ検証' do
      let(:default_conf_hash) do
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
        default_conf_hash.delete(:format)
        expect { test_driver.configure(hash_to_conf default_conf_hash) }.to raise_error(Fluent::ConfigError)
      end

      it 'work_dir が指定されない場合に例外が発生すること' do
        default_conf_hash.delete(:work_dir)
        expect { test_driver.configure(hash_to_conf default_conf_hash) }.to raise_error(Fluent::ConfigError)
      end

      it 's3_bucket が指定されない場合に例外が発生すること' do
        default_conf_hash.delete(:s3_bucket)
        expect { test_driver.configure(hash_to_conf default_conf_hash) }.to raise_error(Fluent::ConfigError)
      end

      it 's3_prefix が指定されない場合に例外が発生すること' do
        default_conf_hash.delete(:s3_prefix)
        expect { test_driver.configure(hash_to_conf default_conf_hash) }.to raise_error(Fluent::ConfigError)
      end
    end

    context 'AWS regionパラメータ検証' do
      before(:each) { enable_iam_role }

      it 'region が指定されない かつ regionが取得できない場合に例外が発生すること' do
        uri = URI.parse('http://localhost/dummy')
        http = Net::HTTP.new(uri.host, uri.port)
        allow(http).to receive(:get).and_return(nil)
        allow(Net::HTTP).to receive(:new).and_return(http)
        expect { test_driver.configure(default_conf) }.to raise_error(Fluent::ConfigError)
      end

      it 'region が指定されない かつ regionが取得できる場合に例外が発生しないこと' do
        enable_region_mock
        expect { test_driver.configure(default_conf) }.not_to raise_error
      end

      it 'region が指定された場合に regionを取得しない かつ 例外が発生しないこと' do
        conf = %(
          region ap-northeast-1
        ) + default_conf
        expect { test_driver.configure(conf) }.not_to raise_error
      end
    end

    context 'AWS 証明書パラメータ検証' do
      before(:each) { enable_region_mock }

      it 'IAMRole が無い かつ access_key_id と secret_access_key が両方指定された場合に例外が発生しないこと' do
        disable_iam_role
        conf = %(
          access_key_id dummy
          secret_access_key dummy
        ) + default_conf
        expect { test_driver.configure(conf) }.not_to raise_error
      end

      it 'IAMRole が有る かつ access_key_id と secret_access_key が両方指定された場合に例外が発生しないこと' do
        enable_iam_role
        conf = %(
          access_key_id dummy
          secret_access_key dummy
        ) + default_conf
        expect { test_driver.configure(conf) }.not_to raise_error
      end

      it 'IAMRole が有る かつ access_key_id と secret_access_key が両方指定されない場合に例外が発生しないこと' do
        enable_iam_role
        expect { test_driver.configure(default_conf) }.not_to raise_error
      end

      it 'IAMRole が無い かつ access_key_id と secret_access_key が両方指定されない場合に例外が発生すること' do
        disable_iam_role
        expect { test_driver.configure(default_conf) }.to raise_error(Fluent::ConfigError)
      end

      it 'IAMRole が無い かつ access_key_id が指定される かつ secret_access_key が指定されない場合に例外が発生すること' do
        disable_iam_role
        conf = %(
          access_key_id dummy
        ) + default_conf
        expect { test_driver.configure(conf) }.to raise_error(Fluent::ConfigError)
      end

      it 'IAMRole が無い かつ access_key_id が指定されない かつ secret_access_key が指定される場合に例外が発生すること' do
        disable_iam_role
        conf = %(
          secret_access_key dummy
        ) + default_conf
        expect { test_driver.configure(conf) }.to raise_error(Fluent::ConfigError)
      end

      it 'IAMRole が有る かつ access_key_id が指定される かつ secret_access_key が指定されない場合に例外が発生すること' do
        enable_iam_role
        conf = %(
          access_key_id dummy
        ) + default_conf
        expect { test_driver.configure(conf) }.to raise_error(Fluent::ConfigError)
      end

      it 'IAMRole が有る かつ access_key_id が指定されない かつ secret_access_key が指定される場合に例外が発生すること' do
        enable_iam_role
        conf = %(
          secret_access_key dummy
        ) + default_conf
        expect { test_driver.configure(conf) }.to raise_error(Fluent::ConfigError)
      end
    end
  end
end
