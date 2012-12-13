%%
%% @author CHAE, H B <chaehb@gmail.com>
%% @copyright 2012 CHAE, H B.
%%
%% @doc supervisor for pooling couchbase connections
%%
-module(erlcb_pool).
-author('Chae,H B<chaehb@gmail.com>').

-behaviour(supervisor).

-include("erlcb.hrl").

-define(CHILD_MODULE, erlcb_couchbase_sup).

%% API
-export([start_link/0, stop/0]).

-export([add/1, remove/1]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->	
	process_flag(trap_exit, true),

	%case ets:info(erlcb_pool) of
	%	undefined ->
			ets:new(erlcb_pool,[named_table,{keypos,#erlcb_couchbase.name},public]),
	%	_->
	%		ok
	%end,

    {ok, { {one_for_one, 5, 10}, []} }.

%% @doc stop this supervisor
stop()->
	case whereis(?MODULE) of
		Pool when is_pid(Pool) ->
			exit(Pool,kill);
		_->
			ok
	end.
	
%% @doc add and remove new couchbase connection to pool
%% 
-spec add(CouchbaseConfig) 
	-> Response :: term() when CouchbaseConfig :: #erlcb_couchbase{} .
add(
		#erlcb_couchbase{
			name=Name,
			protocol=Protocol,
			host=Host,
			port=Port,
			bucket=[BucketName,BucketPassword],
			view=ViewType } = CouchbaseConfig
		) when is_record(CouchbaseConfig,erlcb_couchbase) ->
	if
		(Name =:= undefined ) or (is_atom(Name) =:= false) ->
			{error, bad_config};
		(Protocol =:= undefined ) or (is_atom(Protocol) =:= false) ->
			{error, bad_config};
		(Host =:= undefined) or (is_list(Host) =:= false) ->
			{error, bad_config};
		(Port =:= undefined) or (is_integer(Port) =:= false) ->
			{error, bad_config};
		(BucketName =:= undefined) or (is_list(BucketName) =:= false) ->
			{error, bad_config};
		(BucketPassword =:= undefined) or (is_list(BucketPassword) =:= false) ->
			{error, bad_config};
		(ViewType =/= production) and (ViewType =/= development) ->
			{error, bad_config};
		true ->
			case ets:lookup(erlcb_pool,Name) of
				[] ->
					ets:insert(erlcb_pool,CouchbaseConfig#erlcb_couchbase{streamingUri= "/pools/default/bucketsStreaming/"++BucketName}),
					
					CouchbaseSupName=erlcb_util:append_to_atom(Name,'_couchbase_sup'),
					ChildSpec = {CouchbaseSupName, {?CHILD_MODULE, start_link, [Name]},
								permanent,5000,supervisor,[?CHILD_MODULE]},
					supervisor:start_child(?MODULE,ChildSpec);
				_->
					{error, already_exists_name}
			end
	end;

add(_)->
	{error, bad_config}.

remove(CouchbaseName)->
	CouchbaseSupName=erlcb_util:append_to_atom(CouchbaseName,'_sup'),
	ets:delete(erlcb_pool,CouchbaseName),
	supervisor:terminate_child(?MODULE,CouchbaseSupName),
	supervisor:delete_child(?MODULE,CouchbaseSupName).
	
