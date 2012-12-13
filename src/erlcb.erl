%%
%% @author CHAE, H B <chaehb@gmail.com>
%% @copyright 2012 CHAE, H B.
%%
%% @doc client module
%%
-module(erlcb).
-author('Chae,H B<chaehb@gmail.com>').

-behaviour(gen_server).

-include("erlcb.hrl").
-include("erlcb_memcache.hrl").

-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).
-export([start/0,start_config/1, start_servers/1,start_server/4,start_server/5]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([new/0,new_server/4,new_server/5,new_servers/1,new_config/1]).

-export([stats/1,stat/2]).
-export([get/2, multi_get/2]).

-export([add/4,set/4,replace/4,delete/2]).

-export([touch/3,get_and_touch/3]).

-export([increment/5,decrement/5]).

-export([append/4,prepend/4]).

-export([view/4]).

-export([exists/1]).
%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

start()->
	application:start(crypto),
	application:start(inets),
	application:start(erlcb),

	new().
	
start_servers(Servers)->
	application:start(crypto),
	application:start(inets),
	application:start(erlcb),
	
	new_servers(Servers).
	
start_config(Config)->
	try
		{ok,[Servers]}=file:consult(Config),
		start_servers(Servers)
	catch
		_:_->
			{error,bad_config}
	end.
	
start_server(Id,Url,Bucket,Password)->
	start_server(Id,Url,Bucket,Password,production).
	
start_server(Id,Url,Bucket,Password,ViewType)->
	application:start(crypto),
	application:start(inets),
	application:start(erlcb),
	
	new_server(Id,Url,Bucket,Password,ViewType).	

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(_Args) ->
	process_flag(trap_exit, true),
    {ok, []}.

handle_call({connect,CouchbaseServers},_From, State)->
	connect(CouchbaseServers),
	{reply, ok, State};

%% @doc return -> { StateCode :: integer() | Count :: integer(), Value :: binary() }
handle_call({request,{ServerName,Request}},_From,State)->
	Reply=gen_server:call(ServerName,Request),
	{reply,Reply,State};
					
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% erlcb API Function Definitions
%% ------------------------------------------------------------------

%% @doc load default config file in ${app}/priv/erlcb.conf
%% example ) erlcb.conf
%% [
%%   {
%%		Id :: atom(), connection id
%%		Url :: string(), base url e.g. http://localhost:8091
%%		Bucket :: string(), bucket name
%%		Password :: string(), bucket password
%%		ViewType :: atom() production or development
%%   },
%%	 ...
%% ]
new() ->
	try
		{ok,[Servers]}=file:consult("priv/erlcb.config"),
		new_servers(Servers)
	catch
		_:_->
			{error,bad_config}
	end.
	

new_server(Id,Url,Bucket,Password) ->
	new_server(Id,Url,Bucket,Password,production).

new_server(Id,Url,Bucket,Password,ViewType)->
	new_servers([{Id,Url,Bucket,Password,ViewType}]).

new_config(Config)->
	try
		{ok,[Servers]}=file:consult(Config),
		new_servers(Servers)
	catch
		_:_->
			{error,bad_config}
	end.
	
new_servers(Servers) ->
	try
		CouchbaseServers = lists:map(
			fun({Id,Url,Bucket,Password,ViewType})->
				case is_atom(Id) and is_atom(ViewType) of
					false ->
						throw(bad_config);
					true ->
						ok
				end,
				
				case ViewType of
					production ->
						ok;
					development ->
						ok;
					_->
						throw(bad_config)
				end,
				
				case string:tokens(Url,"://") of
					[Prot,Host,Prt] ->
						Protocol = case list_to_atom(Prot) of
							http ->
								http;
							https ->
								https;
							_->
								throw(bad_config)
						end,
						Port = list_to_integer(Prt),
						#erlcb_couchbase{
							name=Id,
							protocol=Protocol,
							host=Host,
							port=Port,
							bucket=[Bucket,Password],
							view=ViewType,
							streamingUri="/pools/default/bucketsStreaming/"++Bucket
						};
					_->
						throw(bad_config)
				end	
			end,
			Servers
		),
		
		gen_server:call(?MODULE,{connect,CouchbaseServers})
	catch
		_:Reason->
			{error, Reason}
	end.
	
%% ------------------------------------------------------------------
%% erlcb Memcache Protocol API Function Definitions
%% ------------------------------------------------------------------
stats(ServerName) ->
%	case exists(ServerName) of
%		true ->
%			try
%				gen_server:call(?MODULE,{request,{ServerName,{stat,{}}}})
%			catch
%				_:_ ->
%					{?CB_UNEXPECTED_ERROR,<<"unexpected error">>}
%			end;
%		false ->
%			{?CB_CONNECTION_NOT_FOUND, <<"connection not found">>}
%	end.
	{?CB_UNIMPLEMENTED,<<"unimplemented">>}.
	
stat(ServerName,StatName) ->
%	case exists(ServerName) of
%		true ->
%			try
%				gen_server:call(?MODULE,{request,{ServerName,{stat,{}}}})
%			catch
%				_:_ ->
%					{?CB_UNEXPECTED_ERROR,<<"unexpected error">>}
%			end;
%		false ->
%			{?CB_CONNECTION_NOT_FOUND, <<"connection not found">>}
%	end.
	{?CB_UNIMPLEMENTED,<<"unimplemented">>}.
	
%% @doc ServerName :: atom, Key :: binary() 
%% return { ok | error, binary()}
%%
get(ServerName,Key) when is_binary(Key) ->
	case exists(ServerName) of
		true ->
			try
				gen_server:call(?MODULE,{request,{ServerName,{get,{Key}}}})
			catch
				_:_ ->
					{?CB_UNEXPECTED_ERROR,<<"unexpected error">>}
			end;
		false ->
			{?CB_CONNECTION_NOT_FOUND, <<"connection not found">>}
	end.

%% @doc ServerName :: atom, Keys :: list(binary())
%%
multi_get(ServerName,Keys) when is_list(Keys)->
	case exists(ServerName) of
		true ->
			try
				gen_server:call(?MODULE,{request,{ServerName,{multi_get,{Keys}}}})
			catch
				_:_ ->
					{?CB_UNEXPECTED_ERROR,<<"unexpected error">>}
			end;
		false ->
			{?CB_CONNECTION_NOT_FOUND, <<"connection not found">>}
	end.
			

%% @doc Expiry :: integer(), Value :: binary()
add(ServerName,Key,Expiry,Value) when (is_binary(Key) and is_integer(Expiry)) and is_binary(Value) ->
	case exists(ServerName) of
		true ->
			try
				gen_server:call(?MODULE,{request,{ServerName,{add,{Key,Expiry,Value}}}})
			catch
				_:_ ->
					{?CB_UNEXPECTED_ERROR,<<"unexpected error">>}
			end;
		false ->
			{?CB_CONNECTION_NOT_FOUND, <<"connection not found">>}
	end.

set(ServerName,Key,Expiry,Value) when (is_binary(Key) and is_integer(Expiry)) and is_binary(Value) ->
	case exists(ServerName) of
		true ->
			try
				gen_server:call(?MODULE,{request,{ServerName,{set,{Key,Expiry,Value}}}})
			catch
				_:_ ->
					{?CB_UNEXPECTED_ERROR,<<"unexpected error">>}
			end;
		false ->
			{?CB_CONNECTION_NOT_FOUND, <<"connection not found">>}
	end.

replace(ServerName,Key,Expiry,Value) when (is_binary(Key) and is_integer(Expiry)) and is_binary(Value) ->
	case exists(ServerName) of
		true ->
			try
				gen_server:call(?MODULE,{request,{ServerName,{replace,{Key,Expiry,Value}}}})
			catch
				_:_ ->
					{?CB_UNEXPECTED_ERROR,<<"unexpected error">>}
			end;
		false ->
			{?CB_CONNECTION_NOT_FOUND, <<"connection not found">>}
	end.

delete(ServerName,Key) when is_binary(Key) ->
	case exists(ServerName) of
		true ->
			try
				gen_server:call(?MODULE,{request,{ServerName,{delete,{Key}}}})
			catch
				_:_ ->
					{?CB_UNEXPECTED_ERROR,<<"unexpected error">>}
			end;
		false ->
			{?CB_CONNECTION_NOT_FOUND, <<"connection not found">>}
	end.

%% if Expiry = 0, deleted
touch(ServerName,Key,Expiry) when is_binary(Key) and is_integer(Expiry) ->
	case exists(ServerName) of
		true ->
			try
				gen_server:call(?MODULE,{request,{ServerName,{touch,{Key,Expiry}}}})
			catch
				_:_ ->
					{?CB_UNEXPECTED_ERROR,<<"unexpected error">>}
			end;
		false ->
			{?CB_CONNECTION_NOT_FOUND, <<"connection not found">>}
	end.

%% if Expiry = 0, deleted
get_and_touch(ServerName,Key,Expiry) when is_binary(Key) and is_integer(Expiry) ->
	case exists(ServerName) of
		true ->
			try
				gen_server:call(?MODULE,{request,{ServerName,{get_and_touch,{Key,Expiry}}}})
			catch
				_:_ ->
					{?CB_UNEXPECTED_ERROR,<<"unexpected error">>}
			end;
		false ->
			{?CB_CONNECTION_NOT_FOUND, <<"connection not found">>}
	end.

increment(_ServerName,_Key,_Offset,_Default,_Expiry)->
	{?CB_UNIMPLEMENTED,<<"unimplemented">>}.

decrement(_ServerName,_Key,_Offset,_Default,_Expiry)->
	{?CB_UNIMPLEMENTED,<<"unimplemented">>}.
	
append(_ServerName,_Cas,_Key,_Value)->
	{?CB_UNIMPLEMENTED,<<"unimplemented">>}.
	
prepend(_ServerName,_Cas,_Key,_Value)->
	{?CB_UNIMPLEMENTED,<<"unimplemented">>}.


%% ------------------------------------------------------------------
%% erlcb  Couchapi View API Function Definitions
%% ------------------------------------------------------------------

%% @doc Design :: string(), View :: string(), Query :: #erlcb_view_query{}
%% 
view(ServerName,Design,View,Query) 
		when (is_atom(ServerName) and is_record(Query,erlcb_view_query)) and (is_list(Design) and is_list(View)) ->
	case exists(ServerName) of
		true ->
			gen_server:call(?MODULE,{request,{ServerName,{view,{Design,View,Query}}}});
		false ->
			{?CB_CONNECTION_NOT_FOUND, <<"connection not found">>}
	end;
	
view(_S,_D,_V,_Q)->
	{?CB_BAD_REQUEST, <<"bad_args">>}.
	
%% ------------------------------------------------------------------
%% Server Connection Management Internal Function Definitions
%% ------------------------------------------------------------------


connect(CouchbaseServers) ->
	lists:foreach(
		fun(CouchbaseServer)->
			erlcb_pool:add(CouchbaseServer)
		end,
		CouchbaseServers
	).

exists(ServerName) when is_atom(ServerName) ->
	case ets:lookup(erlcb_pool,ServerName) of
		[] ->
			false;
		[_]->
			true
	end;
	
exists(_)->
	false.	

%% ------------------------------------------------------------------
%% Couchapi View Function Definitions
%% ------------------------------------------------------------------

