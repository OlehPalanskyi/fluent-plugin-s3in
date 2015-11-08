require 'fakes3'
require 'tmpdir'
require 'glint'
require 'singleton'
require 'pp'
require 'aws-sdk'
#require 'webmock/rspec'

#WebMock.disable_net_connect!(:allow_localhost => true)

class FakeS3Server
  include Singleton

  define_method(:endpoint) { @endpoint }
  define_method(:dir) { @dir }

  def initialize
    @pid = nil
    @endpoint = nil
    @dir = nil
  end

  def start(dir: nil)
    @dir = dir.nil? ? Dir.mktmpdir : dir
    shutdown
    server = Glint::Server.new(nil, signals: [:INT]) do |port|
      @pid = spawn("bundle exec fakes3 -p #{port} -r #{@dir}  >/dev/null 2>&1")
      # @pid = spawn("bundle exec fakes3 -p #{port} -r #{@dir}")
    end
    # Don't use 127.0.0.1
    @endpoint = 'http://localhost:' + server.port.to_s + '/'
    server.start
  end

  def restart(dir: @dir, delete_dir: false)
    shutdown(delete_dir: delete_dir)
    start(dir: dir)
  end

  def shutdown(delete_dir: true)
    FileUtils.remove_entry_secure(@dir) if delete_dir && Dir.exist?(@dir)
    return if @pid.nil?
    Process.kill('SIGINT', @pid)
    Process.waitpid2(@pid)
    @pid = nil
  end
end
