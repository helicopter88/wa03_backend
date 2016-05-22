require 'pg'
class DatabaseQueries
    def initialize(dbname)
        @conn = PG.connect(dbname: dbname)
    end
    
    def check_instr(instr, name)
    	return @conn.exec("SELECT * FROM instruments WHERE instr_id = '#{instr}' AND name = '#{name}'").ntuples == 1
    end
    
    def check_user(user, psw)
    	return @conn.exec("SELECT * FROM users WHERE user_id = '#{user}' AND pword = '#{psw}'").ntuples == 1
    end

    def insert_user(user, psw, capital)
    	@conn.exec("INSERT INTO users VALUES('#{user}', '#{psw}', '#{capital}')") unless check_user(user, psw)
    end

    def insert_instrument(instr, name)
        @conn.exec("INSERT INTO intruments VALUES('#{instr}', '#{name}')") unless check_instr(instr, name)
    end

    def insert_trans(user, instr, price, amount, type)
    end

    def get_user_capital(user)
	@conn.exec("SELECT capital FROM users WHERE user_id = '#{user}'") do |result|
		result.each do |row|
			return row.values_at('capital')
		end
	end
    end

    def get_total_profit(user)
    end
    
    def get_buy_trans(user)
    end

    def update_user_capital(user, capital)
    	@conn.exec("start transaction; UPDATE users SET capital = '#{capital}' WHERE user_id = '#{user}'; commit;")
    end
end
