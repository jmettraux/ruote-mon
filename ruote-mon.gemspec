
Gem::Specification.new do |s|

  s.name = 'ruote-mon'

  s.version = File.read(
    File.expand_path('../lib/ruote/mon/version.rb', __FILE__)
  ).match(/ VERSION *= *['"]([^'"]+)/)[1]

  s.platform = Gem::Platform::RUBY
  s.authors = [ 'John Mettraux' ]
  s.email = [ 'jmettraux@gmail.com' ]
  s.homepage = 'http://ruote.rubyforge.org'
  s.rubyforge_project = 'ruote'
  s.summary = 'MongoDB storage for ruote (a Ruby workflow engine)'
  s.description = %q{
MongoDB storage for ruote (a Ruby workflow engine)
}

  #s.files = `git ls-files`.split("\n")
  s.files = Dir[
    'Rakefile',
    'lib/**/*.rb', 'spec/**/*.rb', 'test/**/*.rb',
    '*.gemspec', '*.txt', '*.rdoc', '*.md'
  ]

  # s.add_runtime_dependency 'bson_ext'#, '>= 1.4.0'
  # s.add_runtime_dependency 'mongo'#, '>= 1.4.0'
  s.add_runtime_dependency 'ruote', ">= #{s.version.to_s.split('.')[0, 3].join('.')}"

  s.add_development_dependency 'rake'

  s.require_path = 'lib'
end

