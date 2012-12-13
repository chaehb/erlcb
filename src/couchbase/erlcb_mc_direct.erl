%%
%% @author CHAE, H B <chaehb@gmail.com>
%% @copyright 2012 CHAE, H B.
%%
%% @doc memcache direct protocol processing module
%%
-module(erlcb_mc_direct).

-author('Chae,H B<chaehb@gmail.com>').

-behaviour(gen_server).

-include("erlcb_memcache.hrl").
-include("erlcb.hrl").
%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/2]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(CouchbaseName,ModuleName) ->
    gen_server:start_link({local,ModuleName},?MODULE, [CouchbaseName,ModuleName], []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([CouchbaseName,ModuleName]) ->
	process_flag(trap_exit, true),

	NodeIndex= list_to_integer(lists:last(string:tokens(atom_to_list(ModuleName), "_"))),
	[CouchbaseData]=ets:lookup(erlcb_pool,CouchbaseName),
	[Bucket,Password]=CouchbaseData#erlcb_couchbase.bucket,
	
	NodeData=lists:nth(NodeIndex+1,CouchbaseData#erlcb_couchbase.node),
	Host = NodeData#erlcb_node.host,
	Port = NodeData#erlcb_node.direct,
	State=erlcb_mc_protocol:connect([Host,Port,Bucket,Password]),
	
    {ok, State}.
	
handle_call({stat,{}},_From,State)->
	erlcb_mc_protocol:op(stat,{},State);
	
handle_call({Op,{Request,VBucket}},_From,State)->
	erlcb_mc_protocol:op(Op,{Request,VBucket},State);

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
	io:format("~p : terminate...~n",[State#erlcb_memcache.host]),
	case is_port(State#erlcb_memcache.socket) of
		true ->
			gen_tcp:close(State#erlcb_memcache.socket),
			io:format("~p : connection closed.~n",[State#erlcb_memcache.host]),
			ok;
		false ->
			io:format("~p : connection already closed.~n",[State#erlcb_memcache.host]),
			ok
	end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
