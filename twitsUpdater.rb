# frozen_string_literal: true

require 'json'
require 'typhoeus'
require 'awesome_print'
require 'mongo'
require 'slop'
require 'date'
require 'ruby-progressbar'

# grab options from command line
opts = Slop.parse do |o|
  o.string '--uri', 'connection string'
  o.string '-m', '--metacoll', 'name of the meta collection', default: 'meta'
  o.string '-t', '--tweetcoll', 'name of the tweets collection', default: 'tweets'
end

# create a connection to the database
TWDB = Mongo::Client.new(opts[:uri])

# set the logger level for the mongo driver
Mongo::Logger.logger.level = ::Logger::WARN

# Pull the connections in from the MetaColl
connections = TWDB[opts[:metacoll]].find(type: 'connection').first

@bearer_token = connections['bearer_token']
@tweet_lookup_url = connections['tweet_lookup_url']

loop do
  paramsdoc = TWDB[opts[:metacoll]].find(type: 'update_params').first
  params = { "ids": paramsdoc['ids'], "expansions": paramsdoc['expansions'], "tweet.fields": paramsdoc['tweet'],
             "user.fields": paramsdoc['user'] }




  tweets = TWDB[opts[:tweetcoll]].aggregate([
                                              { '$match' => { 'meta' => { '$exists' => false } } },
                                              { '$addFields' => { 'fixedDate' => { '$dateFromString' => { 'dateString' => '$created_at' } } } },
                                              { '$match' => { 'fixedDate' => { '$lt' => (Time.now - paramsdoc['tweetAgeToUpdateSecs']) } } },
                                              { '$limit' => paramsdoc['limit'] },
                                            ])
#                                             { '$sort' => {'created_at' => -1}}
#                                             { '$match' => { 'fixedDate' => { '$lt' => DateTime.now - 1.0 } } },

  numTweets = tweets.count
  puts("--- Found #{numTweets} Tweets to process")
  pb = ProgressBar.create(format: "%a %b\u{15E7}%i %p%% %t %E %c/%C", progress_mark: ' ',
                          remainder_mark: "\u{FF65}", starting_at: 0, total: numTweets)

  tweets.each do |tweet|
    params[:ids] = tweet['id']

    def tweet_lookup(url, bearer_token, params)
      options = {
        method: 'get',
        headers: {
          "User-Agent": 'v2TweetLookupRuby',
          "Authorization": "Bearer #{bearer_token}"
        },
        params: params
      }

      request = Typhoeus::Request.new(url, options)
      request.run
    end

    response = tweet_lookup(@tweet_lookup_url, @bearer_token, params)
    # puts response.code, JSON.pretty_generate(JSON.parse(response.body))
    # ap response
    resetTime = Time.at(response.headers['x-rate-limit-reset'].to_i) - Time.now
    puts("#{response.headers['x-rate-limit-remaining']} | #{resetTime.to_i} |  ")

    update = JSON.parse(response.body).to_h

    if response.headers['x-rate-limit-remaining'].to_i < 2
      puts("--- Sleeping for #{resetTime.to_i} seconds")
      sleep(resetTime.to_i)
    end

    if response.code.to_i != 200
      puts '--- ERRRORRRORR'
      break
    end

    tweet['public_metrics'] = update.values.first[0]['public_metrics']

    updateMeta = {}
    updateMeta[:updateTime] = Time.now
    updateMeta[:lastResponseCode] = response.code
    tweet[:meta] = updateMeta

    # ap tweet
    # puts("--- #{tweet['text']}")
    # write to the mongo document with the new update
    TWDB[opts[:tweetcoll]].update_one({ id: tweet['id'] }, tweet)
    pb.increment
    sleep(paramsdoc['sleep'])
  end
end
