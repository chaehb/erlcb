-module(erlcb_util).

-export([append_to_atom/2]).

-export([uuid/0]).

append_to_atom(A,B) when is_atom(A) and is_atom(B) ->
	list_to_atom(atom_to_list(A) ++ atom_to_list(B));
	
append_to_atom(A,B) when is_atom(A) and is_integer(B) ->
	list_to_atom(atom_to_list(A) ++ integer_to_list(B));

append_to_atom(A,B) when is_atom(A) and is_list(B) ->
	list_to_atom(atom_to_list(A) ++ B ).

uuid()->
    Uuid=uuid:to_string(uuid:uuid4()),
    re:replace(Uuid,"[-\\n]","",[global,{return,binary}]).
