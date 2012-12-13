%%
%% @author CHAE, H B <chaehb@gmail.com>
%% @copyright 2012 CHAE, H B.
%%
%% @doc supervisor for each couchbase server
%% one_for_one
%%
-module(erlcb_couchbase_sup).
-author('Chae,H B<chaehb@gmail.com>').

-behaviour(supervisor).

-include("erlcb.hrl").

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

%% @doc Name :: atom() id
start_link(CouchbaseName) ->
	ServerName=erlcb_util:append_to_atom(CouchbaseName,'_couchbase_sup'),
    supervisor:start_link({local,ServerName},?MODULE, CouchbaseName).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(CouchbaseName) ->
	process_flag(trap_exit, true),

	Specs=children_specs(CouchbaseName),
	
    {ok, { {one_for_one, 5, 10}, Specs } }.


%% ===================================================================
%% Internal functions
%% ===================================================================
	
children_specs(CouchbaseName)->
	[client_spec(CouchbaseName), mc_direct_spec(CouchbaseName), mc_proxy_spec(CouchbaseName), couchapi_spec(CouchbaseName), fsm_spec(CouchbaseName)].
	
client_spec(CouchbaseName)->
	{
		CouchbaseName,
		{
			erlcb_couchbase,start_link,[CouchbaseName]
		},
		permanent,5000,worker,
		[erlcb_couchbase]
	}.

fsm_spec(CouchbaseName)->
	{
		erlcb_util:append_to_atom(CouchbaseName,'_fsm'),
		{
			erlcb_couchbase_fsm,start_link,[CouchbaseName]
		},
		permanent,5000,worker,
		[erlcb_couchbase_fsm]
	}.

mc_direct_spec(CouchbaseName)->
	{
		erlcb_util:append_to_atom(CouchbaseName,'_mc_direct_sup'),
		{
			erlcb_mc_direct_sup,start_link,[CouchbaseName]
		},
		permanent,5000,supervisor,
		[erlcb_mc_direct_sup]
	}.

mc_proxy_spec(CouchbaseName)->
	{
		erlcb_util:append_to_atom(CouchbaseName,'_mc_proxy_sup'),
		{
			erlcb_mc_proxy_sup,start_link,[CouchbaseName]
		},
		permanent,5000,supervisor,
		[erlcb_mc_proxy_sup]
	}.

couchapi_spec(CouchbaseName)->
	{
		erlcb_util:append_to_atom(CouchbaseName,'_couchapi_sup'),
		{
			erlcb_couchapi_sup,start_link,[CouchbaseName]
		},
		permanent,5000,supervisor,
		[erlcb_couchapi_sup]
	}.