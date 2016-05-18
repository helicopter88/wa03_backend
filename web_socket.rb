#!/usr/bin/ruby
require './yahoo_rest'
require 'em-websocket'
EM.run do
  EM::WebSocket.run(host: 'localhost', port: 8080) do |ws|
    ws.onopen do |handshake|
      puts 'HELLO MOM'
      ws.send "Привет мой друг #{handshake.path}"
    end

    ws.onclose { puts 'GOODBYE MOM' }

    ws.onmessage do |msg|
      msg.chop!
      tokens = msg.split
      case tokens[0]
      when 'ask_price'
        ws.send requestAsk(tokens[1])
      when 'bid_price'
        ws.send requestBid(tokens[1])
      when 'exists'
        ws.send checkExistance(tokens[1])
      else
        ws.send "спасибо за письмо: #{msg}"
      end
    end
  end
end
