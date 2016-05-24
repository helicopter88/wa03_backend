require 'pg'
require_relative 'yahoo_rest'
class DatabaseQueries
  def initialize(dbname)
    @conn = PG.connect(dbname: dbname)
  end

  def check_instr(instr, name)
    @conn.exec("SELECT * FROM instruments WHERE instr_id = '#{instr}' AND name = '#{name}'").ntuples == 1
  end

  def users_instr(user, instr)
    q = @conn.exec("SELECT amount FROM owned WHERE user_id = '#{user}' AND instr_id = '#{instr}")
    return 0 if q.ntuples == 0
    q.getvalue(0, 0).to_i
  end

  def check_user(user, psw)
    @conn.exec("SELECT * FROM users WHERE user_id = '#{user}' AND pword = '#{psw}'").ntuples == 1
  end

  def insert_user(user, psw, capital)
    @conn.exec("INSERT INTO users VALUES('#{user}', '#{psw}', '#{capital}', '#{capital}')") unless check_user(user, psw)
  end

  def insert_instrument(instr, name)
    @conn.exec("INSERT INTO instruments VALUES('#{instr}', '#{name}')") unless check_instr(instr, name)
  end

  def current_amount(user, instr)
    curr = @conn.exec("SELECT amount
			    FROM owned
			    WHERE user_id = '#{user}'
				AND instr_id = '#{instr}'")
    return 0 if curr.ntuples == 0
    curr.getvalue(0, 0).to_i
  end

  # type: true for buy, false for sell
  def insert_trans(user, instr, price, amount, type)
    u_capital = get_user_capital(user)
    value = price * amount
    curr = current_amount(user, instr)
    currency = get_se(instr)
    if type == 't'
      if value <= u_capital
        @conn.exec(
          "start transaction;
      		 UPDATE users SET capital = '#{u_capital - value}' WHERE user_id = '#{user}';
      		 INSERT INTO trans (user_id, instr_id, price, amount, type, time, currency)
      			 VALUES ('#{user}', '#{instr}', '#{price}', '#{amount}', '#{type}', clock_timestamp(), '#{currency}');
      		 INSERT INTO owned VALUES ('#{user}', '#{instr}', '#{amount}', '#{currency}')
      		 ON CONFLICT (user_id, instr_id) DO UPDATE SET amount = #{curr + amount};
      		 commit;"
        )
        return true
      end
      return false
    else
      if curr >= amount
          @conn.exec(
          "start transaction;
      		 INSERT INTO trans (user_id, instr_id, price, amount, type, time)
      			 VALUES ('#{user}', '#{instr}', '#{price}', '#{amount}', '#{type}', clock_timestamp());
      		 UPDATE ONLY users SET capital = #{u_capital + value} WHERE user_id = '#{user}';
      		 UPDATE ONLY owned SET amount = #{curr - amount} WHERE user_id = '#{user}' AND instr_id = '#{instr}';
      		 DELETE FROM ONLY owned WHERE amount = 0;
      		commit;"
        )
        return true
      end
      return false
   end
  end

  def get_user_capital(user)
    @conn.exec("SELECT capital FROM users WHERE user_id = '#{user}'").getvalue(0, 0).to_i
  end

  # TODO: total profit = current capital + any unrealised pnl (for all owned instr, mult amount by current price)
  def get_total_profit(user)
  get_user_capital(user) + get_unrealised_pnl(user)
  end

  def get_unrealised_pnl(user)
  upnl = 0
  # To get the upnl, we go over the list of owned instruments and get the yahoo price
  yr = YahooRest.new
  q = @conn.exec("SELECT * FROM owned WHERE user_id = '#{user}'")
  q.each do |row|
    upnl += yr.request_bid(row['instr_id']).to_f * row['amount'].to_i
  end
  upnl
  end    

  def get_sell_price(symbol)
  yr = YahooRest.new
  return yr.request_bid(symbol)
  end

  def get_buy_price(symbol)
  yr = YahooRest.new
  return yr.request_ask(symbol)
  end

  def get_buy_trans(user)
    # TODO: make an array of (user_id, instr_id, amount, price) and return as JSON file
    q = @conn.exec("SELECT * FROM trans WHERE user_id = '#{user}' AND type = 't'")
    puts "'USER' 'INSTR' 'AMOUNT' 'PRICE' 'TIME' 'CURRENCY'"
    q.each do |row|
      puts '%s %s %d %f %s %s' % [row['user_id'], row['instr_id'], row['amount'], row['price'], row['time'], row['currency']]
    end
  end

  def get_sell_trans(user)
    # TODO: make an array of (user_id, instr_id, amount, price) and return as JSON file
    q = @conn.exec("SELECT * FROM trans WHERE user_id = '#{user}' AND type = 'f'")
    puts "'USER' 'INSTR' 'AMOUNT' 'PRICE', 'TIME', 'CURRENCY'"
    q.each do |row|
      puts '%s %s %d %f %s %s' % [row['user_id'], row['instr_id'], row['amount'], row['price'], row['time'], row['currency']]
    end
  end

  def get_current_instr(user)
    q = @conn.exec("SELECT * FROM owned WHERE user_id = '#{user}'")
    puts "'USER' 'INSTR' 'AMOUNT', 'CURRENCY'"
    q.each do |row|
      puts '%s %s %d %s' % [row['user_id'], row['instr_id'], row['amount'], row['currency']]
    end
  end

  def update_user_capital(user, capital)
    @conn.exec("start transaction; UPDATE users SET capital = '#{capital}' WHERE user_id = '#{user}'; commit;")
  end

  def clean_transactions(user)
    @conn.exec("DELETE FROM ONLY trans WHERE user_id = '#{user}'")
  end

  def get_se(instr)
  yr = YahooRest.new
  se = yr.retrieve_se(instr)
  if se.include? "LSE"
    c = "GBP"
  elsif se.include? "NME" or se.include? "NYQ"
    c = "USD"
  end
  return c
  end

end
