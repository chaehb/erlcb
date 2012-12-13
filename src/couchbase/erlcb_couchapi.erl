%%
%% @author CHAE, H B <chaehb@gmail.com>
%% @copyright 2012 CHAE, H B.
%%
%% @doc couchapi protocol processing module
%%
-module(erlcb_couchapi).

-author('Chae,H B<chaehb@gmail.com>').

-behaviour(gen_server).

-include("erlcb.hrl").
-include("erlcb_http.hrl").

-record(erlcb_couchapi,{base,auth}).
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
	CouchApiBase=NodeData#erlcb_node.couchApi,
	Auth = case length(Password) > 0 of
		true ->
			erlcb_http:basic_auth_header(Bucket,Password) ;
		false ->
			[]
	end,
	io:format("View Server Ready for ~p : ~ts~n",[CouchbaseName, CouchApiBase]),
    {ok, #erlcb_couchapi{ base=CouchApiBase, auth=Auth } }.

handle_call({fetch,{Design,View,Query}},_From, State)->
	Reply=fetch(State,{Design,View,Query}),
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
%% Internal Function Definitions
%% ------------------------------------------------------------------
fetch(State,{Design,View,Query}) ->
	Url = State#erlcb_couchapi.base ++ Design++"/_view/"++View,
%	{ok,Body}=erlcb_view:encode_json(Query),
%	io:format("~p~n",[Body]),
%	erlcb_http:post(Url,State#erlcb_couchapi.auth,[],"application/json",Body).
	{ok,Params}=erlcb_view:encode_query(Query),
	erlcb_http:get(Url++"?"++Params,State#erlcb_couchapi.auth,[]).
