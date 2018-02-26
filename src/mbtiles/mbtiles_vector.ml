(** Copyright (C) 2018,  Gavin J Stark.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @file      mbtiles_vector.ml
 * @brief     Decode mbtiles vector tiles
 *
 * The PBF spec used here was from https://github.com/mapbox/vector-tile-spec/ v2.1

        // Variant type encoding
        // The use of values is described in section 4.1 of the specification
        message Value {
                // Exactly one of these values must be present in a valid message
                optional string string_value = 1;
                optional float float_value = 2;
                optional double double_value = 3;
                optional int64 int_value = 4;
                optional uint64 uint_value = 5;
                optional sint64 sint_value = 6;
                optional bool bool_value = 7;

                extensions 8 to max;
        }

        // Features are described in section 4.2 of the specification
        message Feature {
                optional uint64 id = 1 [ default = 0 ];

                // Tags of this feature are encoded as repeated pairs of
                // integers.
                // A detailed description of tags is located in sections
                // 4.2 and 4.4 of the specification
                repeated uint32 tags = 2 [ packed = true ];

                // The type of geometry stored in this feature.
                optional GeomType type = 3 [ default = UNKNOWN ];

                // Contains a stream of commands and parameters (vertices).
                // A detailed description on geometry encoding is located in 
                // section 4.3 of the specification.
                repeated uint32 geometry = 4 [ packed = true ];
        }

        // Layers are described in section 4.1 of the specification
        message Layer {
                // Any compliant implementation must first read the version
                // number encoded in this message and choose the correct
                // implementation for this version number before proceeding to
                // decode other parts of this message.
                required uint32 version = 15 [ default = 1 ];

                required string name = 1;

                // The actual features in this tile.
                repeated Feature features = 2;

                // Dictionary encoding for keys
                repeated string keys = 3;

                // Dictionary encoding for values
                repeated Value values = 4;

                // Although this is an "optional" field it is required by the specification.
                // See https://github.com/mapbox/vector-tile-spec/issues/47
                optional uint32 extent = 5 [ default = 4096 ];

                extensions 16 to max;
        }

        repeated Layer layers = 3;

        extensions 16 to 8191;
}

 *)

(*a Libraries *)
open Batteries

(*a Types *)
(*t t_pbf *)
type t_pbf = Pbf.t_pbf

(*t t_ba_char, t_ba_uint32 *)
type t_ba_char     = (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t
type t_ba_uint32   = (int32, Bigarray.int32_elt, Bigarray.c_layout) Bigarray.Array1.t
type t_ba_float32  = (float, Bigarray.float32_elt, Bigarray.c_layout) Bigarray.Array1.t

(*f ba_char, ba_uint32 *)
let ba_char    size = Bigarray.(Array1.create char c_layout size)
let ba_uint32  size = Bigarray.(Array1.create int32 c_layout size)
let ba_float32 size = Bigarray.(Array1.create float32 c_layout size)

(*t t_geomtype *)
type t_geomtype = Unknown | Point | Line | Polygon | MultiPolygon | ConvexPolygon | Rectangle

(*a Useful functions *)
let sfmt = Printf.sprintf

(*a Value, KeyValue, Feature, Layer *)
(*m Geometry *)
module Geometry = struct
  (*t t structure - the immutable result *)
  type t = {
      geom_type : t_geomtype;
      data_floats: t_ba_float32;
      steps: int array; (* steps is coord<<12 | (count<<4) | 1 (lineto) / 2 (moveto) / 7 (closepath) *)
    }

  (*f geom_type getter *)
  let geom_type t = t.geom_type

  (*f coords getter *)
  let coords t = t.data_floats

  (*f steps getter *)
  let steps t = t.steps

  (*t t_build structure - used when parsing data from vector tile *)
  type t_build = {
    data_uint32 : t_ba_uint32;
    len : int;
    next_block : int;
    extent : float;
    mutable geom_type: t_geomtype; (* basically a path-through, but might detect a rectangle *)
    mutable cursor : float*float;
    mutable rev_coords : (float * float) list;
    mutable num_coords : int;
    mutable rev_steps : int list;
    mutable path_open_coord: int;    
    }

  (*t add_step - add a step to the path *)
  let add_step t cmd count coord =
    let step = (coord lsl 12) lor (count lsl 4) lor cmd in
    t.rev_steps <- step :: t.rev_steps

  (*t add_coord - add a coordinate pair to the path *)
  let add_coord t coords =
    t.num_coords <- t.num_coords + 1;
    t.rev_coords <- coords :: t.rev_coords;
    t.cursor <- coords

  (*t coord - get a coordinate from a uint32 *)
  let coord t ui32 =
    let value = Int32.(to_float (shift_right_logical ui32 1)) in
    if (Int32.(equal (logand ui32 1l) 1l)) then (-1. *. value /. t.extent) else (value /. t.extent)

  (*t add_count - add a number of 'cmd' and coordinate pairs to the path *)
  let add_count t ofs cmd count =
    if (ofs+2*count<=t.next_block) then (
      add_step t cmd count t.num_coords;
      for i=0 to count-1 do
        let dx = coord t t.data_uint32.{ofs+2*i+0} in
        let dy = coord t t.data_uint32.{ofs+2*i+1} in
        let (cx,cy) = t.cursor in
        let pt = (cx+.dx, cy+.dy) in
        let n = t.num_coords in
        add_coord t pt
      done;
      if (cmd==1) then t.path_open_coord <- max 0 (t.num_coords-1);
    );
    ofs+2*count

  (*t add_close_path - add closing of a path *)
  let add_close_path t ofs =
    add_step t 7 0 t.path_open_coord;
    ofs

  (*t handle_cmd - handle a command from the vector tile uint32s, then loop *)
  let rec handle_cmd t ofs is_first =
    if (ofs>=t.next_block) then t else (
      let cmd_int = t.data_uint32.{ofs} in
      let cmd = (Int32.to_int cmd_int) land 7 in
      let count = Int32.(to_int (shift_right_logical cmd_int 3)) in
      match cmd with
      | 1 -> (
            if ((not is_first) && (t.geom_type=Polygon)) then (t.geom_type <- MultiPolygon);
            handle_cmd t (add_count t (ofs+1) 1 count) false (* move to *)
      )
      | 2 -> handle_cmd t (add_count t (ofs+1) 2 count) false (* line to *)
      | 7 -> handle_cmd t (add_close_path t (ofs+1)) false (* close *)
      | _ -> t
    )

  (*f just_build - build from the command set *)
  let just_build t ofs =
    Some (handle_cmd t ofs true)

  (*f try_rectangle 
     rectangle is polygon of len=11, 1 (of 1) x y 2 (of 3) dx0 dy0 dx1 dy1 -dx0 -dx1 (7 of 0) 
   *)
  let try_rectangle t ofs =
    if ((t.len==11) &&
        (Int32.equal t.data_uint32.{ofs+0} 0x09l) &&
        (Int32.equal t.data_uint32.{ofs+3} 0x1al) &&
        (Int32.equal (Int32.logand 7l t.data_uint32.{ofs+10}) 7l) &&
        (Int32.equal (Int32.logxor t.data_uint32.{ofs+4} t.data_uint32.{ofs+8}) 1l) &&
        (Int32.equal (Int32.logxor t.data_uint32.{ofs+5} t.data_uint32.{ofs+9}) 1l) ) then (
      t.geom_type <- Rectangle;
      add_coord t (coord t t.data_uint32.{ofs+1}, coord t t.data_uint32.{ofs+2});
      add_coord t (coord t t.data_uint32.{ofs+4}, coord t t.data_uint32.{ofs+5});
      add_coord t (coord t t.data_uint32.{ofs+6}, coord t t.data_uint32.{ofs+7});
      Some t
    ) else (
      None
    )
    
  (*f try_convex_polygon
     determine if it is a single polygon that is convex
    triangle is moveto x y drawto x y x y close which means min length is 9
   *)
  let try_convex_polygon t ofs =
    let num_pts_if_single = (t.len-3)/2 in
    let is_convex = 
      if ((num_pts_if_single>3) &&
            (Int32.equal t.data_uint32.{ofs+0} 0x09l) && (* moveto *)
              (Int32.equal (Int32.of_int (((num_pts_if_single-1) lsl 3) lor 2)) t.data_uint32.{ofs+ 3}) && (* lineto of num_pts_if_single*)
                (Int32.equal (Int32.logand 7l t.data_uint32.{ofs+num_pts_if_single*2+2}) 7l) ) then  (* last is closepath *)
        (
          let rec is_convex n ofs_n dxn dyn =
            if (n==num_pts_if_single) then true else (
              let (dxnp1,dynp1) = (coord t t.data_uint32.{ofs_n}), (coord t t.data_uint32.{ofs_n+1}) in
              let side_of_line = compare (dxn *. dynp1) (dxnp1 *. dyn) in
              if (side_of_line<0) then (
                false
              ) else (
                is_convex (n+1) (ofs_n+2) dxnp1 dynp1
              )
            )
          in
          (* Ignore the moveto coordinate as that does not effect concavity *)
          let (dxn,dyn)     = (     (coord t t.data_uint32.{ofs+4}),       (coord t t.data_uint32.{ofs+5}))   in
          is_convex 2 (ofs+6) dxn dyn
        ) else (
        false
      )
    in
    if is_convex then (
      t.geom_type <- ConvexPolygon;
      just_build t ofs
    ) else (
      None
    )
    
  (*t create_from_maptile_geometry - build from vector tile uint32s then cast immutably *)
  let create_from_maptile_geometry ?extent:(extent=4096) geom_type ba ofs len =
    let build = { data_uint32=ba;
                  extent = float extent;
                  len = len;
                  next_block=ofs+len;
                  cursor = (0.,0.);
                  rev_coords=[];
                  rev_steps=[];
                  num_coords=0;
                  path_open_coord=0;
                  geom_type = geom_type;
                }
    in
    let try_x opt_build f build ofs =
      match opt_build with
      | None -> f build ofs
      | b -> b
    in
    let opt_build = None in
    let opt_build = try_x opt_build try_convex_polygon build ofs in
    let opt_build = try_x opt_build try_rectangle      build ofs in
    let opt_build = try_x opt_build just_build         build ofs in
    let build = Option.get opt_build in
    let n = build.num_coords in
    let data_floats = ba_float32 (2*n) in
    let steps = Array.of_list (List.rev build.rev_steps) in
    let geom_type = build.geom_type in
    let rec populate_coords i cs =
      match cs with
      | [] -> ()
      | (x,y)::tl -> (
        data_floats.{2*i+0} <- x;
        data_floats.{2*i+1} <- y;
        populate_coords (i-1) tl
      )
    in
    populate_coords (n-1) build.rev_coords;
    {
      geom_type;
      data_floats;
      steps;
    }

  (*f display *)
  let display t =
    let num_steps = Array.length t.steps in
    let num_points = (Bigarray.Array1.dim t.data_floats) / 2 in
    Printf.printf "Geometry %d steps with %d points\n" num_steps num_points;
    ()
    
  (*f All done *)    
end

(*m Value *)
module Value = struct
  exception ValueError of string
  (*t t variant *)
  type t = 
    | Float of float
    | Int of int64
    | Text of string
    | Bool of bool

  (*f as_int *)
  let as_int v =
    match v with 
    | Float f -> int_of_float f
    | Int x   -> Int64.to_int x
    | Bool b  -> if b then 1 else 0
    | _ -> raise (ValueError "as_int")

  (*f as_string *)
  let as_string v =
    match v with 
    | Text s  -> s
    | _ -> raise (ValueError "as_string")

  (*f as_float *)
  let as_float v =
    match v with 
    | Float f -> f
    | Int x   -> Int64.to_float x
    | Bool b  -> if b then 1. else 0.
    | _ -> raise (ValueError "as_float")

  (*f pbf_type_size_fns *)
  let pbf_type_size_fns = [ (1L, Pbf.PbfString    (fun _ s -> let l = min 239 (String.length s) in (l, l)));
                            (2L, Pbf.PbfUint32    (fun _ s -> (249, 4))); (* actually float *)
                            (3L, Pbf.PbfUint64    (fun _ s -> (250, 8))); (* actually double *)
                            (4L, Pbf.PbfUint64    (fun _ s -> (251, 8))); (* actually int64 *)
                            (5L, Pbf.PbfUint64    (fun _ s -> (252, 8))); (* actually uin64 *)
                            (6L, Pbf.PbfUint64    (fun _ s -> (253, 8))); (* actually sint64 *)
                            (7L, Pbf.PbfBool      (fun _ s -> if s then (255,0) else (254,0)));
                     ]

  (*f parse_pbf_type_size - get type and size of a value in a protobuf *)
  let parse_pbf_type_size pbf ofs len = 
    Pbf.fold_message pbf_type_size_fns (0,0) pbf ofs len

  (*f populate_bytes *)
  let populate_bytes n (ba,data_ofs) v =
    for i=0 to (n-1) do
      let i8 = 8*i in
      ba.{data_ofs+i} <- Char.chr Int64.(to_int (logand (shift_right v i8) 0xffL));
    done;
    (ba,data_ofs)

  (*f populate_string *)
  let populate_string (ba,data_ofs) s =
    let n = min 239 (String.length s) in
    for i=0 to (n-1) do
      ba.{data_ofs+i} <- s.[i];
    done;
    (ba,data_ofs)
    
  (*f pbf_populate_fns *)
  let pbf_populate_fns = [ (1L, Pbf.PbfString    (populate_string));
                           (2L, Pbf.PbfUint64    (populate_bytes 4)); (* actually float *)
                           (3L, Pbf.PbfUint64    (populate_bytes 8)); (* actually double *)
                           (4L, Pbf.PbfUint64    (populate_bytes 8)); (* actually int64 *)
                           (5L, Pbf.PbfUint64    (populate_bytes 8)); (* actually uin64 *)
                           (6L, Pbf.PbfUint64    (populate_bytes 8)); (* actually sint64 *)
                           (7L, Pbf.PbfBool      (fun a _ -> a));
                     ]

  (*f parse_pbf_populate - get type and size of a value in a protobuf *)
  let parse_pbf_populate ba data_ofs pbf ofs len = 
    Pbf.fold_message pbf_populate_fns (ba,data_ofs) pbf ofs len

  (*f data_ref *)
  let data_ref data_ofs value_type = 
    Int32.of_int ((data_ofs lsl 8) lor value_type)

  (*f get_value *)
  let get_value ba data_ref =
    let value_type = (Int32.to_int data_ref) land 0xff in
    let data_ofs   = Int32.(to_int (shift_right_logical data_ref 8)) in
    let rec read_n acc n =
      if (n<=0) then acc else (
        let new_n = n-1 in
        let data_ofs = data_ofs+new_n in
        let new_acc = Int64.(logor (shift_left acc 8) (of_int (Char.code (ba.{data_ofs})))) in
        read_n new_acc new_n
      )
    in
    let read_string l =
      String.init l (fun i -> ba.{data_ofs+i})
    in
    match value_type with
    | 255 -> Bool true
    | 254 -> Bool false
    | 253 -> Int (read_n 0L 8)
    | 252 -> Int (read_n 0L 8)
    | 251 -> Int (read_n 0L 8)
    | 250 -> Float (Int64.float_of_bits (read_n 0L 8))
    | 249 -> Float (Int32.float_of_bits (Int64.to_int32 (read_n 0L 4)))
    | l -> Text (read_string l)
  
  (*f str *)
  let str v =
    match v with
    | Float f -> sfmt "%f" f
    | Int i   -> sfmt "%Ld" i
    | Text s  -> sfmt "'%s'" s
    | Bool b  -> sfmt "%b" b

  (*f All done *)
end

(*m KeyValue *)
module KeyValue = struct
  (*t t structure *)
  type t = {
    ba           : t_ba_char;
    ofs          : int;
    len          : int;
    value_uint32 : int32;
    }

  (*f create *)
  let create ba ofs len value_uint32 =
    { ba; ofs; len; value_uint32; }

  (*f key_equals_len *)
  let key_equals_len t len name =
    if len!=t.len then false else (
      let rec cmp_chr i =
        if (i<0) then true
        else if (t.ba.{t.ofs+i} != name.[i]) then false
        else cmp_chr (i-1)
      in
      cmp_chr (len-1)
    )

  (*f key_equals *)
  let key_equals t name =
    key_equals_len t (String.length name) name

  (*f get_value *)
  let get_value t =
    let value = Value.get_value t.ba t.value_uint32 in
    value

  (*f strs *)
  let strs t =
    let value = Value.get_value t.ba t.value_uint32 in
    (String.init t.len (fun i->t.ba.{t.ofs+i})), (Value.str value)

  (*f All done *)
end


(*m Feature *)
module Feature = struct

  (*t t structure *)
  type t = {
      uid : int;
      geom_type : t_geomtype;
      tag_ol  : int*int;
      geom_ol : int*int;
    }
  type t_feature = t

  (*m Analysis submodule *)
  module Analysis = struct
    (*t t structure *)
    type t = {
        mutable uid     : int;
        mutable geom_type       : t_geomtype;
        mutable num_tag_uint32  : int;
        mutable num_geom_uint32 : int;
      }

    (*f create *)
    let create _ = { uid=0; geom_type=Unknown; num_tag_uint32=0; num_geom_uint32=0; }

    (*f num_uint32 *)
    let num_uint32 a = a.num_tag_uint32 + a.num_geom_uint32

    (*f analyse_uid *)
    let analyse_uid a i = a.uid <- i; a

    (*f analyse_tags *)
    let analyse_tags a uia = 
      a.num_tag_uint32 <- a.num_tag_uint32 + (Array.length uia); a

    (*f analyse_geom *)
    let analyse_geom a uia =
      a.num_geom_uint32 <- a.num_geom_uint32 + (Array.length uia); a

    (*f analyse_geom_type *)
    let analyse_geom_type a i =
      a.geom_type <- (match i with
                      | 1 -> Point
                      | 2 -> Line
                      | 3 -> Polygon
                      | _ -> Unknown );
      a

    (*f analyse_fns *)
    let analyse_fns = [ (1L, Pbf.PbfInt       analyse_uid);
                        (2L, Pbf.PbfRepUint32 analyse_tags);
                        (4L, Pbf.PbfRepUint32 analyse_geom);
                        (3L, Pbf.PbfInt       analyse_geom_type);
                      ]

    (*f All done *)
  end

  (*m Populate submodule *)
  module Populate = struct
    (*t t structure *)
    type t = {
        feature     : t_feature;
        map_string  : bool -> int32 -> int32;
        data_uint32 : t_ba_uint32;
        data_chars  : t_ba_char;
      }

    (*f create *)
    let create feature map_string data_uint32 data_chars = 
      {
        feature; map_string; data_uint32; data_chars;
      }

    (*f populate_uints *)
    let populate_uints t uia ol map = 
      let l = Array.length uia in
      let (ofs,len) = ol in
      for i=0 to l-1 do t.data_uint32.{ofs+i} <- map ((i land 1)==0) uia.(i) done;
      t

    (*f populate_geom *)
    let populate_geom t uia = populate_uints t uia t.feature.geom_ol (fun _ x -> x)

    (*f populate_tags *)
    let populate_tags t uia = populate_uints t uia t.feature.tag_ol t.map_string

    (*f populate_fns *)
    let populate_fns = [ (2L, Pbf.PbfRepUint32 populate_tags);
                         (4L, Pbf.PbfRepUint32 populate_geom);
                      ]

    (*f All done *)
  end

  (*f create from analysis and base of uint32s *)
  let create (a:Analysis.t) base = {
      uid = a.uid;
      geom_type = a.geom_type;
      tag_ol  = (base, a.num_tag_uint32);
      geom_ol = (base+a.num_tag_uint32, a.num_geom_uint32);
    }

  (*f parse_pbf_analyse - analyse a feature in a protobuf *)
  let parse_pbf_analyse pbf ofs len uint32_ofs = 
    let af = Analysis.create () in
    let af = Pbf.fold_message Analysis.analyse_fns af pbf ofs len in
    let f = create af uint32_ofs in
    (f, Analysis.num_uint32 af)

  (*f parse_pbf_populate - populate a feature in a protobuf *)
  let parse_pbf_populate t data_uint32 data_chars map_string pbf ofs len = 
    let p = Populate.create t map_string data_uint32 data_chars in
    ignore (Pbf.fold_message Populate.populate_fns p pbf ofs len);
    ()

  (*f uid *)
  let uid t = t.uid

  (*f geom_type *)
  let geom_type t = t.geom_type

  (*f geometry *)
  let geometry ?extent t ba =
    let (ofs,len) = t.geom_ol in
    Geometry.create_from_maptile_geometry ?extent:extent t.geom_type ba ofs len

  (*f kv_find_key *)
  let kv_find_key t get_kv name =
    let (ofs,len) = t.tag_ol in
    let name_len = String.length name in
    let rec seek i = 
      if i>=len then None else (
        let kv = get_kv (2*i+ofs) in
        if (KeyValue.key_equals_len kv name_len name) then (Some kv)
        else (seek (i+1))
      )
    in
    seek 0

  (*f kv_map_default *)
  let kv_map_default t get_kv f default name =
    match kv_find_key t get_kv name with
    | Some kv -> f (KeyValue.get_value kv)
    | None -> default

  (*f kv_fold *)
  let kv_fold t get_kv f acc =
    let (ofs,len) = t.tag_ol in
    let rec fold i acc =
      if i>=len then acc else (
        let new_acc = f acc (get_kv (2*i+ofs)) in
        fold (i+1) new_acc
      )
    in
    fold 0 acc

  (*f kv_iter *)
  let kv_iter t get_kv f =
    let (ofs,len) = t.tag_ol in
    for i=0 to len/2-1 do
      f (get_kv (2*i+ofs))
    done

  (*f kv_count *)
  let kv_count t = 
    let (_,len) = t.tag_ol in
    len/2

  (*f kv_get *)
  let kv_get t get_kv n =
    let (ofs,len) = t.tag_ol in
    get_kv (2*n+ofs)

  (*f display *)
  let display ?indent:(indent="") t data_uint32 get_kv =
    let geom_string = 
      match t.geom_type  with
      | Point -> "points"
      | Line -> "lines"
      | Polygon -> "polygon"
      | ConvexPolygon -> "convex polygon"
      | MultiPolygon -> "multiple polygons (and outer with >=1 inner polygons)"
      | Rectangle -> "rectangle"
      | _ -> "unknown"
    in
    Printf.printf "%sFeature uid(%d) geometry %s\n" indent t.uid geom_string;
    Printf.printf "%s%sNum tags (%d) num geom (%d)\n" indent indent ((snd t.tag_ol)/2) (snd t.geom_ol);
    let g = min (snd t.geom_ol) 12 in
    let ofs = fst t.geom_ol in
    for i=0 to g-1 do
    Printf.printf "(%ld)" data_uint32.{ofs+i};
    done;
    Printf.printf "\n";
    let (ofs,len) = t.tag_ol in
    for i=0 to len/2-1 do
      let (ks, vs) = KeyValue.strs (get_kv (ofs+2*i)) in
      Printf.printf "%s%sTag %d: %s->%s\n" indent indent i ks vs
    done;
    ()

  (*f All done *)
end

(*m Layer module *)
module Layer = struct

  (*t t structure *)
  type t = {
      name:        string;
      data_uint32: t_ba_uint32;
      data_chars:  t_ba_char;
      features:    Feature.t array;    
      extent:      int;
    }
  type t_layer = t

  (*m Analysis submodule *)
  module Analysis = struct

    (*t t structure *)
    type t = {
        mutable layer_version:int; (* Layer version - this code understands v2 only *)
        mutable name:string;       (* Name of layer (e.g. water, transportation, etc) *)
        mutable extent:int;
        mutable num_uint32:int;    (* Number of uint32s required for the layer *)
        mutable num_chars:int;     (* Number of chars required for the layer *)
        mutable num_features:int;  (* Number of features - will equal length of feature_list *)
        mutable feature_list:Feature.t list; (* List of features in the layer (built in reverse order) *)
        mutable keys   : (int*int*int32) list;     (* List of start character/length/ref for keys (built in reverse order) *)
        mutable values : (int*int*int32) list;     (* List of start character/length/ref for values (built in reverse order) *)
      }

    (*f create *)
    let create _ = {
        layer_version = 0;
        name          = "";
        extent        = 4096;
        num_chars     = 0;
        num_uint32    = 0;
        num_features  = 0;
        feature_list  = [];
        keys          = [];
        values        = [];
      }

    (*f finalize
       feature_list is built backwards, so unreverse that
     *)
    let finalize a =
      a.feature_list <- List.rev a.feature_list;
      a

    (*f analyse_name *)
    let analyse_name a s = a.name <- s; a

    (*f analyse_layer_version *)
    let analyse_layer_version a i = a.layer_version <- i; a

    (*f analyse_extent *)
    let analyse_extent a i = a.extent <- i; a

    (*f analyse_feature *)
    let analyse_feature a (pbf,ofs,len) = 
      let (f, num_uint32) = Feature.parse_pbf_analyse pbf ofs len a.num_uint32 in
      a.num_features <- a.num_features + 1;
      a.num_uint32 <- a.num_uint32 + num_uint32;
      a.feature_list <- f::a.feature_list;
      a

    (*f analyse_key *)
    let analyse_key a (pbf,ofs,len) =
      let data_ofs = a.num_chars in
      let data_len = min len 255 in
      let data_ref = Int32.of_int ((data_ofs lsl 8) lor data_len) in
      a.keys <- (data_ofs,data_len,data_ref) :: a.keys;
      a.num_chars <- a.num_chars + data_len;
      a

    (*f analyse_value *)
    let analyse_value a (pbf,ofs,len) =
      let (value_type,value_len) = Value.parse_pbf_type_size pbf ofs len in
      let data_ofs = a.num_chars in
      let data_len = value_len in
      let data_ref = Value.data_ref data_ofs value_type in
      a.values <- (a.num_chars,data_len,data_ref) :: a.values;
      a.num_chars <- a.num_chars + data_len;
      a

    (*f analyse_fns *)
    let analyse_fns = [ (2L,  Pbf.PbfBlob      analyse_feature);
                        (3L,  Pbf.PbfBlob      analyse_key);
                        (4L,  Pbf.PbfBlob      analyse_value);
                        (1L,  Pbf.PbfString    analyse_name);
                        (15L, Pbf.PbfInt       analyse_layer_version);
                        (5L,  Pbf.PbfInt       analyse_extent);
                      ]

    (*f All done *)
  end

  (*m Populate submodule *)
  module Populate = struct

    (*t t structure *)
    type t = {
        layer  : t_layer;
        keys   : (int*int*int32) array;
        values : (int*int*int32) array;
        mutable feature : int;
        mutable key     : int;
        mutable value   : int;
      }

    (*f create *)
    let create layer rev_key_list rev_value_list =
      let keys   = Array.of_list (List.rev rev_key_list) in
      let values = Array.of_list (List.rev rev_value_list) in
      {
        layer;
        keys;
        values;
        feature = 0;
        key = 0;
        value = 0;
      }

    (*f populate_feature *)
    let populate_feature t (pbf,ofs,len) = 
      let l = t.layer in
      let f = l.features.(t.feature) in
      let map_string is_key n =
        let n = Int32.to_int n in
        if is_key then (
          if (n<0) || (n>=Array.length t.keys) then 0l
          else (
            let (_,_,data_ref) = t.keys.(n)  in
            data_ref
          )
        ) else (
          if (n<0) || (n>=Array.length t.values) then 0l
          else (
            let (_,_,data_ref) = t.values.(n)  in
            data_ref
          )
        )
      in
      Feature.parse_pbf_populate f l.data_uint32 l.data_chars map_string pbf ofs len;
      t.feature <- t.feature + 1;
      t

    (*f populate_key *)
    let populate_key t s = 
      let ba = t.layer.data_chars in
      let (data_ofs,data_len,_) = t.keys.(t.key) in
      for i=0 to (data_len-1) do ba.{data_ofs+i} <- s.[i] done;
      t.key <- t.key + 1;
      t

    (*f populate_value
      A value is a pbf message that (here) has N (0-255) bytes of data plus a type
      The
     *)
    let populate_value t (pbf,ofs,len) = 
      let ba = t.layer.data_chars in
      let (data_ofs,data_len,_) = t.values.(t.value) in
      ignore (Value.parse_pbf_populate ba data_ofs pbf ofs len);
      t.value <- t.value + 1;
      t

    (*f populate_fns - does not handle 5L extent *)
    let populate_fns = [ (2L, Pbf.PbfBlob    populate_feature);
                         (3L, Pbf.PbfString  populate_key);
                         (4L, Pbf.PbfBlob    populate_value);
                      ]

    (*f All done *)
  end

  (*f name *)
  let name t = t.name

  (*f features *)
  let features t = t.features

  (*f parse_pbf - analyse a layer in a protobuf *)
  let parse_pbf layer_filter_fn pbf ofs len = 
    let af = Analysis.create () in
    let af = Pbf.fold_message Analysis.analyse_fns af pbf ofs len in
    let af = Analysis.finalize af in
    if (af.layer_version!=2) then None
    else if not (layer_filter_fn af.name) then None
    else (
      let name = af.name in
      let extent = max 1 af.extent in
      let data_uint32 = ba_uint32 af.num_uint32 in
      let data_chars  = ba_char   af.num_chars in
      let features = Array.of_list af.feature_list in
      let t = {name; extent; data_uint32; data_chars; features} in
      let p = Populate.create t af.keys af.values in
      ignore (Pbf.fold_message Populate.populate_fns p pbf ofs len);
      Some t
    )
    
  (*f byte_size - estimate size in bytes *)
  let byte_size t =
    (Bigarray.Array1.size_in_bytes t.data_uint32) + (Bigarray.Array1.size_in_bytes t.data_chars) + 64*(Array.length t.features)

  (*f key_ofs_len - mirrors analyse_key and populate_key *)
  let key_ofs_len data_ref =
    let len = (Int32.to_int data_ref) land 0xff in
    let ofs = Int32.(to_int (shift_right_logical data_ref 8)) in
    (ofs, len)

  (*f get_kv *)
  let get_kv t kv_ofs =
    let (ofs,len) = key_ofs_len t.data_uint32.{kv_ofs} in
    KeyValue.create t.data_chars ofs len t.data_uint32.{kv_ofs+1}

  (*f feature_geometry *)
  let feature_geometry t f =
    Feature.geometry ~extent:t.extent f t.data_uint32

  (*f feature_kv_find_key *)
  let feature_kv_find_key t f name =
    Feature.kv_find_key f (get_kv t) name

  (*f feature_kv_map_default *)
  let feature_kv_map_default t f fn default name =
    Feature.kv_map_default f (get_kv t) fn default name

  (*f feature_kv_fold *)
  let feature_kv_fold t f fn acc =
    Feature.kv_fold f (get_kv t) fn acc

  (*f feature_kv_find_key *)
  let feature_kv_find_key t f name =
    Feature.kv_find_key f (get_kv t) name

  (*f feature_kv_iter *)
  let feature_kv_iter t f fn =
    Feature.kv_iter f (get_kv t) fn

  (*f feature_kv_count *)
  let feature_kv_count t f =
    Feature.kv_count f

  (*f feature_kv_get *)
  let feature_kv_get t f n =
    Feature.kv_get f (get_kv t) n

  (*f feature_iter *)
  let feature_iter t f =
    Array.iter f t.features

  (*f feature_fold *)
  let feature_fold t f acc =
    Array.fold_left f acc t.features

  (*f feature_get *)
  let feature_get t n =
    t.features.(n)

  (*f feature_count *)
  let feature_count t =
    Array.length t.features

  (*f display *)
  let display ?all:(all=false) t =
    Printf.printf "Layer '%s' est bytes %d num_uint32 %d num_chars %d num_features %d\n" t.name (byte_size t) (Bigarray.Array1.dim t.data_uint32) (Bigarray.Array1.dim t.data_chars) (Array.length t.features);
    if all then (
      Array.iter (fun f -> Feature.display ~indent:"  " f t.data_uint32 (get_kv t)) t.features;
    );
    ()

  (*f All done *)
end

(*a Tile module *)
(*m Tile *)
module Tile = struct

  (*t t structure *)
  type t = {
    mutable layers: Layer.t array;
    layer_filter_fn: string -> bool;
    }

  (*t create *)
  let create ?layer_filter_fn _ =
    let layer_filter_fn = Option.default (fun _->true) layer_filter_fn in
    {
      layers=Array.of_list [];
      layer_filter_fn;
    }

  (*f parse_layer *)
  let parse_layer (t,acc) (pbf,ofs,len) =
    match (Layer.parse_pbf t.layer_filter_fn pbf ofs len) with
    | None -> (t,acc)
    | Some layer -> 
       Layer.display layer;
       (t,(layer::acc))

  (*f parse_fns *)
  let parse_fns = [ (3L, Pbf.PbfBlob parse_layer); ]

  (*f parse_pbf *)
  let parse_pbf t pbf =
    let pbf_size = Bigarray.Array1.dim pbf in
    let (_,layers) = Pbf.fold_message parse_fns (t,[]) pbf 0 pbf_size in
    t.layers <- Array.of_list layers

  (*f byte_size - estimate size in bytes *)
  let byte_size t =
    Array.fold_left (fun a l -> a + (Layer.byte_size l)) 0 t.layers

  (*f feature_geometry *)
  let feature_geometry = Layer.feature_geometry

  (*f feature_kv_find_key *)
  let feature_kv_find_key = Layer.feature_kv_find_key

  (*f feature_kv_map_default *)
  let feature_kv_map_default = Layer.feature_kv_map_default

  (*f feature_kv_fold *)
  let feature_kv_fold = Layer.feature_kv_fold

  (*f feature_kv_iter *)
  let feature_kv_iter = Layer.feature_kv_iter

  (*f feature_kv_count *)
  let feature_kv_count = Layer.feature_kv_count

  (*f feature_kv_get *)
  let feature_kv_get = Layer.feature_kv_get

  (*f feature_iter *)
  let feature_iter = Layer.feature_iter

  (*f feature_fold *)
  let feature_fold = Layer.feature_fold

  (*f feature_get *)
  let feature_get = Layer.feature_get

  (*f feature_count *)
  let feature_count = Layer.feature_count

  (*f layer_iter *)
  let layer_iter t f =
    Array.iter f t.layers

  (*f get_layer *)
  let get_layer t name =
    Array.fold_left (fun acc l -> if (String.equal l.Layer.name name) then (Some l) else acc) None t.layers 

  (*f All done *)
end

(*a Useful functions sometimes *)
let show_pbf pbf =
    let pbf_size = Bigarray.Array1.dim pbf in
    for i=0 to (pbf_size/16-1)/100 do
      Printf.printf "%4d:" (i*16);
      for j=0 to 16 do
        Printf.printf " %02x" (Char.code pbf.{i*16+j});
      done;
      Printf.printf "\n";
    done

