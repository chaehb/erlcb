%%
%% @author CHAE, H B <chaehb@gmail.com>
%% @copyright 2012 CHAE, H B.
%%
%% @doc supervisor for erlcb_couchapi
%%
-module(erlcb_couchapi_sup).
-author('Chae,H B<chaehb@gmail.com>').

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link(CouchbaseName) ->
	Name=erlcb_util:append_to_atom(CouchbaseName,'_couchapi_sup'),
    supervisor:start_link({local, Name},?MODULE, CouchbaseName).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_CouchbaseName) ->
	process_flag(trap_exit, true),
	
    {ok, { {one_for_one, 5, 10}, []} }.
