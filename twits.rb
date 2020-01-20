# frozen_string_literal: true

require 'slop'
require 'tweetstream'
require 'mongo'
require 'awesome_print'
require 'json'

opts = Slop.parse do |o|
  o.string '--uri', 'connection string'
  o.string '-d', '--db', 'database'
  o.string '-m', '--metacoll', 'name of the meta collection', default: 'meta'
end
puts opts

TWDB = Mongo::Client.new(opts[:uri])

# set the logger level for the mongo driver
Mongo::Logger.logger.level = ::Logger::WARN

# Pull the API keys in from the MetaColl
keys = TWDB[:meta].find(type: 'API_KEYS').first
ap keys

# Access for the Twitter streaming client
TweetStream.configure do |config|
  config.consumer_key = keys['consumer_key']
  config.consumer_secret    = keys['consumer_secret']
  config.oauth_token        = keys['oauth_token']
  config.oauth_token_secret = keys['oauth_token_secret']
  config.auth_method        = :oauth
end

# Pull the watchlist keys in from the MetaColl
watches = TWDB[:meta].find(type: 'WATCHES').first

# we're not interested in retweets
def isRetweet?(tweet)
  return true if tweet[0, 2] == 'RT'

  false
end

# Create a tweet stream
TweetStream::Client.new.track(watches[:tags]) do |tweet|
  if isRetweet?(tweet.text)
    puts '-RETWEET-'
  else
    ap "#{tweet.id} -- #{tweet.text}"
    # insert the tweet object into the DB
    # id = DKDB[:tweets].insert_one(tweet.to_h)
  end
end
