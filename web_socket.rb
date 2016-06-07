#!/usr/bin/ruby
require 'socket'
require_relative 'yahoo_rest'
require_relative 'db'
require 'em-websocket'
require_relative 'news'

hostname = Socket.gethostname.strip
puts "Webserver is being hosted on #{hostname}"
yr = YahooRest.new
db = DatabaseQueries.new 'summitdb'
news = News.new
EM.run do
  # for now use localhost, this will be changed later
  EM::WebSocket.run(host: '0.0.0.0', port: 8080) do |ws|
    ws.onopen do |handshake|
      puts "User #{handshake.path} connected!"
    end

    ws.onclose { puts 'User disconnected' }

    ws.onmessage do |msg|
      puts msg
      msg.strip!
      # We want tokens separated by spaces
      tokens = msg.split
      # Format: <yahoo | db> <action> <arguments....>
      case tokens.shift
      when 'yahoo'
        ws.send yr.parse_tokens(tokens)
      when 'db'
        ws.send db.parse_tokens(tokens)
      when 'news'
        ws.send news.parse_tokens(tokens) 
      else
        ws.send "Invalid Message: #{msg}"
      end
    end
  end
end
