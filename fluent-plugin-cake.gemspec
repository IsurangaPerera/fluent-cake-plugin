lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name = 'fluent-plugin-cake'
  spec.version = '0.1.0'
  spec.authors = ['Isuranga Perera']
  spec.email = ['isurangamperera@gmail.com']

  spec.summary = 'Fluentd plugin to reload child plugin\'s config'
  spec.homepage = 'https://github.com/bash0C7/fluent-plugin-config_reloader'
  spec.license = 'Apache-2.0'

  test_files, files = `git ls-files -z`.split("\x0").partition do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.files = files
  spec.executables = files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files = test_files
  spec.require_paths = ['lib']

  spec.add_development_dependency 'bundler', '~> 1.14'
  spec.add_development_dependency 'rake', '~> 12.0'
  spec.add_development_dependency 'test-unit', '~> 3.0'
  spec.add_runtime_dependency 'fluentd', ['>= 0.14.10', '< 2']
  spec.add_development_dependency 'pry', '~> 0'
  spec.add_runtime_dependency 'rest-client', '~> 0'
  spec.add_development_dependency 'rspec', '~> 0'

end