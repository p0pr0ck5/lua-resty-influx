use Test::Nginx::Socket::Lua;

plan tests => 3 * blocks() + 1;

no_shuffle();
run_tests();

__DATA__

=== TEST 1: write_udp success
--- config
	location /t {
		content_by_lua '
			ngx.socket.udp = function() return {
				setpeername = function(host, port) return true end,
				send = function(data) return true end,
			} end

			local util = require "resty.influx.util"

			local ok, err = util.write_udp("foo bar", "127.0.0.1", 8089)

			ngx.say(ok)
			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
true
nil
--- no_error_log
[error]

=== TEST 2: write_udp fails on setpeername
--- config
	location /t {
		content_by_lua '
			ngx.socket.udp = function() return {
				setpeername = function(host, port) return false, "setpeername failure" end,
				send = function(data) return true end,
			} end

			local util = require "resty.influx.util"

			local ok, err = util.write_udp("foo bar", "127.0.0.1", 8089)

			ngx.say(ok)
			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
false
setpeername failure
--- no_error_log
[error]

=== TEST 3: write_udp fails on send
--- config
	location /t {
		content_by_lua '
			ngx.socket.udp = function() return {
				setpeername = function(host, port) return true end,
				send = function(data) return false, "send failure" end,
			} end

			local util = require "resty.influx.util"

			local ok, err = util.write_udp("foo bar", "127.0.0.1", 8089)

			ngx.say(ok)
			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
false
send failure
--- no_error_log
[error]

=== TEST 4: write_http success
--- http_config
server {
	listen 12345;
	location / {
		return 204;
	}
}
--- config
	location /t {
		content_by_lua '
			local util = require "resty.influx.util"

			local opts = {
				host = "127.0.0.1",
				port = 12345,
				db = "testdb",
				hostname = "hostname.tld",
				precision = "s"
			}

			local ok, err = util.write_http("foo bar", opts)

			ngx.say(ok)
			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
true
nil
--- no_error_log
[error]

=== TEST 5: write_http returns false on connection failure
--- config
	lua_socket_log_errors off;

	location /t {
		content_by_lua '
			local util = require "resty.influx.util"

			local opts = {
				host = "127.0.0.1",
				port = 12345,
				db = "testdb",
				hostname = "hostname.tld",
				proto = "udp",
				precision = "s"
			}

			local ok, err = util.write_http("foo bar", opts)

			ngx.say(ok)
			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
false
connection refused
--- no_error_log
[error]

=== TEST 6: write_http returns false on request failure
--- http_config
server {
	listen 12345;
	location / {
		return 444;
	}
}
--- config
	location /t {
		content_by_lua '
			local util = require "resty.influx.util"

			local opts = {
				host = "127.0.0.1",
				port = 12345,
				db = "testdb",
				hostname = "hostname.tld",
				precision = "s"
			}

			local ok, err = util.write_http("foo bar", opts)

			ngx.say(ok)
			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
false
closed
--- no_error_log
[error]

=== TEST 7: write_http returns false on application failure
--- http_config
server {
	listen 12345;
	location / {
		content_by_lua_block { ngx.status = 400; ngx.print("nope!") }
	}
}
--- config
	location /t {
		content_by_lua '
			local util = require "resty.influx.util"

			local opts = {
				host = "127.0.0.1",
				port = 12345,
				db = "testdb",
				hostname = "hostname.tld",
				precision = "s"
			}

			local ok, err = util.write_http("foo bar", opts)

			ngx.say(ok)
			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
false
nope!
--- no_error_log
[error]

=== TEST 8: write_http sets the Authorization header when auth is provided
--- http_config
server {
	listen 12345;
	location / {
		access_by_lua_block { ngx.log(ngx.INFO, ngx.var.http_Authorization) }
		return 204;
	}
}
--- config
	location /t {
		content_by_lua '
			local util = require "resty.influx.util"

			local opts = {
				host = "127.0.0.1",
				port = 12345,
				db = "testdb",
				hostname = "hostname.tld",
				precision = "s",
				auth = "user:pass"
			}

			local ok, err = util.write_http("foo bar", opts)

			ngx.say(ok)
			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
true
nil
--- error_log
Basic dXNlcjpwYXNz
--- no_error_log
[error]
