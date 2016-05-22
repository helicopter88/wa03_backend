#!/usr/bin/ruby
require 'socket'
require_relative 'yahoo_rest'
require 'em-websocket'
hostname = Socket.gethostname.strip
puts "Webserver is being hosted on #{hostname}"
EM.run do
  # for now use localhost, this will be changed later
  EM::WebSocket.run(host: '0.0.0.0', port:8080) do |ws|
    ws.onopen do |handshake|
      puts "User #{handshake.path} connected!"
      ws.send "Welcome #{handshake.path}"
    end

    ws.onclose { puts 'User disconnected' }

    ws.onmessage do |msg|
      msg.strip!
      # We want tokens separated by spaces
      tokens = msg.split
      yr = YahooRest.new
      # The first one is always going to be the action we want to execute
      case tokens[0]
      when 'ask_price'
        ws.send yr.request_ask(tokens[1])
      when 'bid_price'
        ws.send yr.request_bid(tokens[1])
      when 'exists'
        ws.send yr.check_existance(tokens[1])
      else
        ws.send "Invalid Message: #{msg}"
      end
    end
  end
end
