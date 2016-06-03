require 'pg'
require_relative 'yahoo_rest'
require 'json'
class DatabaseQueries
  def initialize(dbname)
    @conn = PG.connect(dbname: dbname)
    @yr = YahooRest.new
    @user_data = user_data
  end

  def parse_tokens(tokens)
    action = tokens[0]
    args = tokens[1..tokens.length]
    "#{action}: #{send(action, *args)}"
  end

  # Check whether the instrument exists in the instrument table
  def check_instr(instr)
    @conn.exec("SELECT *
		FROM instruments
		WHERE instr_id = '#{instr}'").ntuples == 1
  end

  # Check whether the given user owns the given instrument
  # (it has an open position)
  def users_instr(user, instr)
    q = @conn.exec("SELECT amount
		    FROM owned
		    WHERE user_id = '#{user}'
		      AND instr_id = '#{instr}'")
    return 0 if q.ntuples == 0
    q.getvalue(0, 0).to_i
  end

  # Check whether the given user exists in the database
  def login(user, psw)
    @conn.exec("SELECT *
		FROM users
		WHERE user_id = '#{user}'
		  AND pword = '#{psw}'").ntuples == 1
  end

  # Get the name of the given user
  def get_name(user)
    q = @conn.exec("SELECT *
		FROM users
		WHERE user_id = '#{user}'")
    return '' unless q.ntuples == 1
    q.getvalue(0, 5).to_s
  end

  def get_currency(user)
    q = @conn.exec("SELECT currency FROM users WHERE user_id = '#{user}'")
    return nil if q.ntuples == 0
    q.getvalue(0, 0).to_s
  end

  # Deletes an user from the users database, if it exists, otherwise returns false
  def delete_user(user, psw)
    if login(user, psw)
      @conn.exec("DELETE FROM users
		  WHERE user_id = '#{user}' AND pword = '#{psw}'")
      return true
    end
    false
  end

  # Inserts a user into the database if we don't already have their record otherwise returns false
  def insert_user(user, psw, name, capital, currency)
    unless login(user, psw)
      @conn.exec("INSERT INTO users
		VALUES('#{user}', '#{psw}', '#{capital}',
		 '#{capital}', '#{currency}', '#{name}')")
      return true
    end
    false
  end

  # Deletes an instrument from instruments, if it exists, otherwise returns false
  def delete_instrument(instr)
    if check_instr(instr)
      @conn.exec("DELETE FROM instruments WHERE intr_id = '#{instr}'")
      return true
    end
    false
  end

  # Inserts an instrument into the database if we don't already have its record
  def insert_instr(instr)
    unless check_instr(instr)
      @conn.exec("INSERT INTO instruments
		VALUES('#{instr}', 'None')")
      return true
    end
    false
  end

  # Returns the number of shares currently owned (for a given instrument)
  def current_amount(user, instr)
    curr = @conn.exec("SELECT amount
       		       FROM owned
		       WHERE user_id = '#{user}'
	 	  	 AND instr_id = '#{instr}'")
    return 0 if curr.ntuples == 0
    curr.getvalue(0, 0).to_i
  end

  def buy(user, instr, amount, curr_amount, currency)
    price = @yr.ask_price(instr)
    puts price
    return -7 if price < 0
    return -5 if price == 0
    u_capital = get_capital(user).to_f
    value = price * amount
    return -2 if u_capital < value
    @conn.transaction do |con|
      con.exec "UPDATE users
		      SET capital = #{u_capital - value}
		      WHERE user_id = '#{user}'"
      con.exec "INSERT INTO trans
		      (user_id, instr_id, price, amount, type, time, currency)
    		      VALUES ('#{user}', '#{instr}','#{price}','#{amount}',
		      't', clock_timestamp(), '#{currency}')"
      con.exec "INSERT INTO owned VALUES
		      ('#{user}', '#{instr}', '#{amount}', '#{currency}')
    		      ON CONFLICT (user_id, instr_id)
		      DO UPDATE SET amount = #{curr_amount + amount}"
    end
    update_avg(user, instr)
    "You just bought #{amount} share(s) of #{instr} (#{price} per share) for a" \
    " total of #{value} #{currency}."
  end

  def sell(user, instr, amount, curr_amount, currency)
    price = @yr.bid_price(instr)
    return -7 if price <= 0
    u_capital = get_capital(user).to_f
    value = price * amount
    @conn.transaction do |con|
      con.exec "INSERT INTO trans
     (user_id, instr_id, price, amount, type, time, currency)
  		      VALUES ('#{user}', '#{instr}', '#{price}', '#{amount}',
     'f', clock_timestamp(), '#{currency}')"
      con.exec "UPDATE ONLY users SET capital = #{u_capital + value}
		      WHERE user_id = '#{user}'"
      con.exec "UPDATE ONLY owned SET amount = #{curr_amount - amount}
		      WHERE user_id = '#{user}' AND instr_id = '#{instr}'"
      con.exec 'DELETE FROM ONLY owned WHERE amount = 0'
    end
    "You just sold #{amount} share(s) of #{instr} (#{price} per share) for a" \
    " total of #{value} #{currency}."
  end

  # Perform a transaction according to its type. Updates tables accordingly
  def insert_trans(user, instr, amount, type)
    # type -> 't' = buy, 'f' = sell
    # Error codes:
    # -6 - Negative amount
    # -7 - Negative price
    # -3 - Different currency
    # -4 - Amount is zero
    # -5 - Price is zero
    # -1 - Not enough shares
    # -2 - Not enough capital
    # -8 - Any other error
    curr = current_amount(user, instr)
    amount = amount.to_i
    return -6 if amount <= 0
    # If the transaction is made in a different currency than the one we have
    # the account in, we reject it (for now)
    currency = get_se(instr)
    acc_currency = get_currency(user)
    return -3 unless currency == acc_currency
    if type == 't'
      insert_instr(instr)
      return buy(user, instr, amount, curr, currency)
    elsif type == 'f' && curr >= amount
      return sell(user, instr, amount, curr, currency)
    end
    -8
  end

  def update_avg(user, instr)
    q = @conn.exec("SELECT price, amount FROM trans
		    WHERE user_id = '#{user}'
			AND instr_id = '#{instr}'")
    amount = 0
    total = 0
    q.each do |row|
      total += row['price'].to_f * row['amount'].to_f
      amount += row['amount'].to_f
    end
    avg = total / amount
    @conn.exec("UPDATE owned SET avg = #{avg}
	        WHERE user_id = '#{user}' AND instr_id = '#{instr}'")
  end

  # Get user capital if the user exists
  def get_capital(user)
    q = @conn.exec("SELECT capital FROM users WHERE user_id = '#{user}'")
    return 0 if q.ntuples == 0
    '%.3f' % q.getvalue(0, 0).to_f
  end

  # Calculate the total profit by adding up
  # the user capital and the unrealised pnl
  def get_total(user)
    '%.3f' % (get_initial_capital(user).to_f + get_upnl(user).to_f +
    get_profit(user).to_f)
  end

  # Gets the initial capital of the user
  def get_initial_capital(user)
    q = @conn.exec("SELECT initial FROM users WHERE user_id = '#{user}'")
    return 0 if q.ntuples == 0
    '%.3f' % q.getvalue(0, 0).to_f
  end

  # Get the holdings of the user
  def get_holdings(user)
    bought = @conn.exec("SELECT amount, price
			     FROM trans
			     WHERE user_id = '#{user}' AND type = 't'
			     AND instr_id IN (SELECT instr_id
					      FROM owned
					      WHERE user_id = '#{user}')")
    sold = @conn.exec("SELECT amount, price
  			   FROM trans
  			   WHERE user_id = '#{user}' AND type = 'f'
			   AND instr_id IN (SELECT instr_id
					    FROM owned
					    WHERE user_id = '#{user}')")
    b = 0
    s = 0
    bought.each do |row|
      b += row['amount'].to_f * row['price'].to_f
    end
    sold.each do |row|
      s += row['amount'].to_f * row['price'].to_f
    end
    b - s
  end

  # Calculates the profit by deducting the inital investment from the total
  def get_profit_per_instr(user, instr)
    # Step 1: get current amount of shares bought and sold by user
    q = @conn.exec("SELECT amount, price, type
  		  FROM trans
  		  WHERE user_id = '#{user}' AND instr_id = '#{instr}'")
    b = 0
    s = 0
    avgbuy = 0
    avgsell = 0

    q.each do |row|
      if row['type'] == 't'
        b += row['amount'].to_f
        avgbuy += row['amount'].to_f * row['price'].to_f
      else
        s += row['amount'].to_f
        avgsell += row['amount'].to_f * row['price'].to_f
      end
    end
    avgbuy /= b unless b == 0
    avgsell /= s unless s == 0
    (avgsell - avgbuy) * s
  end

  def get_profit(user)
    q = @conn.exec("SELECT DISTINCT ON (instr_id) instr_id
		    FROM trans
		    WHERE user_id = '#{user}'")
    profit = 0
    q.each do |row|
      profit += get_profit_per_instr(user, row['instr_id'])
    end
    profit
  end

  def get_upnl(user)
    '%.3f' % (get_current_val_per_holdings(user).to_f - get_holdings(user).to_f)
  end

  def get_current_val_per_holdings(user)
    cval = 0
    # To get the upnl, we go over the list of owned instruments
    # and get the yahoo price (we request the bid price)
    q = @conn.exec("SELECT * FROM owned WHERE user_id = '#{user}'")
    q.each do |row|
      cval += @yr.bid_price(row['instr_id']).to_f * row['amount'].to_i
    end
    '%.3f' % cval
  end

  def get_owned(user)
    q = @conn.exec("SELECT instr_id, amount, avg FROM owned WHERE user_id = '#{user}'")
    user_owned = []
    q.each do |row|
      sym = row['instr_id']
      user_owned.push(instr: sym, amount: row['amount'], name: (@yr.req_name sym), bp: (@yr.bid_price sym), ap: (@yr.ask_price sym), avg: row['avg'])
    end
    user_owned.to_json
  end

  # Return the sell/bid price from the Yahoo API
  def get_sell_price(symbol)
    @yr.bid_price(symbol)
  end

  # Return the buy/ask price from the Yahoo API
  def get_buy_price(symbol)
    @yr.ask_price(symbol)
  end

  # Get the official name of the instrument from the Yahoo API
  def get_name_instr(symbol)
    @yr.req_name(symbol).strip![1..-2].to_s
  end

  def get_all_trans(user)
    q = @conn.exec("SELECT * FROM trans WHERE user_id = '#{user}' ORDER BY time DESC")
    t = []
    q.each do |row|
      t.push(user_id: row['user_id'], instr_id: row['instr_id'],
             price: row['price'], amount: row['amount'], type: row['type'],
             time: row['time'], currency: row['currency'])
    end
    t.to_json
  end

  # Currently prints all the buys of a given user
  # TODO: make an array and return as JSON file
  def get_buy_trans(user)
    q = @conn.exec("SELECT *
		   FROM trans
		   WHERE user_id = '#{user}'
		     AND type = 't'")

    # q.each do |row|
    #  puts '%s %s %d %f %s %s'.format([row['user_id'], row['instr_id'],
    #                                  row['amount'], row['price'], row['time'], row['currency']])
    # end
  end

  # Currently prints all the sells of a given user
  # TODO: make an array and return as JSON file
  def get_sell_trans(user)
    q = @conn.exec("SELECT *
		   FROM trans
		   WHERE user_id = '#{user}'
		     AND type = 'f'")
    # puts "'USER' 'INSTR' 'AMOUNT' 'PRICE', 'TIME', 'CURRENCY'"
    # q.each do |row|
    #  puts '%s %s %d %f %s %s'.format([row['user_id'], row['instr_id'],
    #                                   row['amount'], row['price'], row['time'], row['currency']])
    # end
  end

  # Prints the current open positions of the given user
  def get_current_instr(user)
    q = @conn.exec("SELECT * FROM owned WHERE user_id = '#{user}'")
    # puts "'USER' 'INSTR' 'AMOUNT', 'CURRENCY'"
    # q.each do |row|
    #  puts '%s %s %d %s'.format([row['user_id'], row['instr_id'],
    #                             row['amount'], row['currency']])
    # end
  end

  # Updates the user capital with the new given capital
  def update_user_cap(user, capital)
    @conn.exec("UPDATE users SET capital = '#{capital}' WHERE user_id = '#{user}'")
  end

  # FOR TESTING ONLY: deletes all the transactions of a user (does not update c)
  def clean_transactions(user)
    @conn.exec("DELETE FROM ONLY trans WHERE user_id = '#{user}'")
  end

  # Gets the currency of the given instrument by checking its stock exchange
  # Currently only supports London Stock Exchange, NASDAQ SE and New York SE
  def get_se(instr)
    # default is GBP
    c = 'GBP'
    se = @yr.retrieve_se(instr)
    c = 'USD' if se.include?('NMS') || se.include?('NYQ')
    c
  end

  def user_data
    q = @conn.exec('SELECT * FROM users')
    puts 'No registered users' if q.ntuples == 0
    # Check if we cached something, if we did,
    # and it's not older than 10m it is good enough
    return @user_data unless @user_data.nil? || Time.new.to_i - @leader_time > 600
    user_data = []
    q.each do |row|
      name = row['name']
      user = row['user_id']
      upnl = get_upnl(user)
      profit = get_profit(user)
      total = get_total(user)
      user_data.push(user_id: user, user: name, upnl: upnl,
                     profit: profit, total: total)
    end
    @user_data = user_data
    @leader_time = Time.new.to_i
    user_data
  end

  def upnl_leaderboard
    udata = user_data
    udata.sort_by { |h| h[:upnl] }
  end

  def profit_leaderboard
    udata = user_data
    udata.sort_by { |h| h[:profit] }
  end

  def total_leaderboard
    udata = user_data
    udata.sort_by { |h| h[:total] }
  end

  def leaderboard(type, user)
    type.downcase!
    if type == 'total'
      l = total_leaderboard
    elsif type == 'profit'
      l = profit_leaderboard
    elsif type == 'upnl'
      l = upnl_leaderboard
    else
      puts "The type can be 'total', 'profit' or 'upnl'"
    end
    fwd = get_followed_users(user)
    l.select! do |e|
      fwd.include?(e[:user_id]) || e[:user_id] == user
    end
    l
  end

  def get_leaderboard(type, user)
    l = leaderboard(type, user).to_json
    # puts ' User  |  Profit  |  upnl  |  Total '
    # l.each do |row|
    #  puts "#{row[:user]} | #{row[:profit].to_f.round(3)} | " \
    # 	  "#{row[:upnl].to_f.round(3)} | #{row[:total].to_f.round(3)}"
    # end
  end

  def get_followed_users(user)
    q = @conn.exec("SELECT user_id FROM follow WHERE followed_by  = '#{user}'")
    followed = []
    q.each do |row|
      followed.push(row['user_id'])
    end
    followed
  end

  #TODO: good naming bro
  def follow(fwd, fws)
    # Follow fee is £10 (or 10$, depending on the account)
    return false if get_capital(fws) == 0
    @conn.transaction do |con|
      con.exec "UPDATE users
		SET capital = capital + 10 WHERE user_id = '#{fwd}'"
      con.exec "UPDATE users
		SET capital = capital - 10 WHERE user_id = '#{fws}'"
      con.exec "INSERT INTO follow VALUES ('#{fwd}', '#{fws}')"
    end
    true
  end

  def get_f_trans(user)
    q = @conn.exec("SELECT * from trans WHERE user_id IN
  			(SELECT user_id FROM follow
  			 WHERE followed_by = '#{user}') OR
  			user_id = '#{user}'")
    trans = []
    q.each do |row|
      trans.push(user: row['user_id'], instr: row['instr_id'],
                 amount: row['amount'], price: row['price'],
                 type: row['type'], time: row['time'],
                 currency: row['currency'])
    end
    trans.sort_by { |h| h[:time] }.reverse!
  end

  def get_rank(user)
    q = leaderboard('profit', user)
    p = 0
    pos = q.index { |h| h[:user_id] == user }
    p = pos unless pos.nil?
    q.size - p
  end

  private :update_user_cap, :clean_transactions
end
