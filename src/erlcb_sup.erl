%%
%% @author CHAE, H B <chaehb@gmail.com>
%% @copyright 2012 CHAE, H B.
%%
%% @doc supervisor for app
%% one_for_one
%%
-module(erlcb_sup).
-author('Chae,H B<chaehb@gmail.com>').

-behaviour(supervisor).

%% API
-export([start_link/0]).

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

	PoolSup={erlcb_pool,{erlcb_pool,start_link,[]},permanent,5000,supervisor,[erlcb_pool]},
	Client={erlcb, {erlcb, start_link, []}, permanent, 5000, worker, [erlcb]},
    {ok, { {one_for_one, 5, 10}, [PoolSup,Client]} }.

