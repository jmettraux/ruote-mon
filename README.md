
# ruote-mon

A MongoDB storage for ruote.

This is the 'official' storage. There is also: https://github.com/PlasticLizard/ruote-mongodb but it contains optimizations for Plastic Lizard's team.

There is also http://github.com/reedlaw/ruote-moped but it's (2013/04/13) in an early stage of development.

Works best with a 1.9.x Ruby.


## usage

```ruby
require 'ruote'
require 'ruote-mon'

ruote = Ruote::Dashboard.new(
  Ruote::Worker.new(
    Ruote::Mon::Storage.new(
      Mongo::Connection.new()['ruote_mon_test'],
      {})))

# ...
```


## running tests

assuming you have checked out side by side

```
ruote/
ruote-mon/
```

Get into ruote/ and make sure you have

```
gem 'mongo'
gem 'bson_ext'
```

in the Gemfile there. Run ```bundle install``` if necessary.

start your MongoDB server and then, from ruote/


* basic tests :

run

```
  RUOTE_STORAGE=mon bundle exec ruby test/functional/storage.rb
```

* functional tests :

get into ruote/ and do

```
  RUOTE_STORAGE=mon bundle exec ruby test/functional/test.rb
```


## license

MIT


## links

* http://ruote.rubyforge.org/
* http://github.com/jmettraux/ruote-mon


## feedback

* mailing list : http://groups.google.com/group/openwferu-users
* bug tracker: http://github.com/jmettraux/ruote-mon/issues
* irc : irc.freenode.net #ruote

