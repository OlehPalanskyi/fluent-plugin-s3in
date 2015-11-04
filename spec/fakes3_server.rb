require 'fakes3'
require 'tmpdir'
require 'glint'
require 'singleton'
require 'pp'

class FakeS3Server
  include Singleton

  define_method(:endpoint) { @endpoint }
  define_method(:dir) { @dir }

  def initialize
    @pid = nil
    @endpoint = nil
    @dir = nil
  end

  def start
    @dir = Dir.mktmpdir
    server = Glint::Server.new(nil, signals: [:INT]) do |port|
      @pid = spawn("bundle exec fakes3 -p #{port} -r #{@dir}")
    end
    @endpoint = 'http://127.0.0.1:' + server.port.to_s + '/'
    server.start
  end

  def shutdown
    return if @pid.nil?
    Process.kill('SIGINT', @pid)
    Process.waitpid2(@pid)
    @pid = nil

    FileUtils.remove_entry_secure(@dir) if Dir.exist?(@dir)
  end
end
