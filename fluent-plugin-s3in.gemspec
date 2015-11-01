# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name          = 'fluent-plugin-s3in'
  spec.version       = File.read('VERSION').strip
  spec.authors       = ['Takeshi Shiihara']
  spec.email         = ['shi-take@dummy.dummy']

  spec.summary       = %q{Write a short summary, because Rubygems requires one.}
  spec.description   = %q{Write a longer description or delete this line.}
  spec.homepage      = 'https://github.com/shii-take/fluent-plugin-s3in'
  spec.license       = 'MIT'

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ['lib']

  spec.required_ruby_version = '~> 2'

  spec.add_dependency 'fluentd', '~> 0'
  spec.add_dependency 'sequel', '~> 4'
  spec.add_dependency 'aws-sdk', '~> 2.1'
  spec.add_dependency 'sqlite3', '~> 1.3'
  spec.add_dependency 'tzinfo', '~> 1.2'

  spec.add_development_dependency 'bundler', '~> 1.0'
  spec.add_development_dependency 'rake', '~> 10.0'
  spec.add_development_dependency 'test-unit', '~> 3.0'
  spec.add_development_dependency 'fakes3'
  spec.add_development_dependency 'glint'
end
