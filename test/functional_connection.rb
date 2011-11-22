
#
# testing ruote-mon
#
# Tue Nov 22 16:54:57 JST 2011
#

require 'ruote-mon'


def new_storage(opts)

  Ruote::Mon::Storage.new(
    Mongo::Connection.new['ruote_mon_test'],
    opts)
end

