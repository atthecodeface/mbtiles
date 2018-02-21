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
 * @file      mbtiles_tiles.ml
 * @brief     Provide access to OpenStreetMap .mbtiles data files    
 *
 * images:
 * "14/8169/10972" (BLOB "\031\213\008\000\000\000\000\000\000\003...")
 * "14/8169/10973" (BLOB "\031\213\008\000\000\000\000\000\000\003...")
 * ...
 * 
 * The BLOBs are zlib-compressed PBF (Google protocol buffer) messages
 *)

(*a Libraries - Batteries for Option *)
open Batteries
module MaptileVectorPbf = Maptile_vector_pbf

(*a Useful functions *)
(*f ba_char *)
type t_ba_char = (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t
let ba_char size = Bigarray.(Array1.create char c_layout size)

(*f sfmt *)
let sfmt = Printf.sprintf

(*f exceptions *)
exception UnhandledZlibIssue

(*a Tile module *)
(*m Tile *)
module Tile = struct
  (*t t structure *)
  type t = {
      name: string;
      x : int;
      y : int;
      (* lat long in nano? *)    
      mutable cached_pbf: t_ba_char option;
    }

  (*f create *)
  let create x y name = 
    {
      x;
      y;
      name;
      cached_pbf = None;
    }

  (*f get_xy - getter for x,y *)
  let get_xy t = (t.x, t.y)

  (*f get_name - getter for name *)
  let get_name t = t.name

  (*f tile_within_bbox *)
  let tile_within_bbox bbox t =
    let (x0,y0,x1,y1) = bbox in
    if      (t.x < x0) then false
    else if (t.y < y0) then false
    else if (t.x >= x1) then false
    else if (t.y >= y1) then false
    else true

  (*f pbf_valid return true if *)
  let pbf_valid t =
    Option.is_some t.cached_pbf

  (*f get_pbf *)
  let get_pbf t =
    Option.get t.cached_pbf

  (*f cache_pbf_from_blob *)
  let zlib_in_ba  = ba_char 65536
  let zlib_out_ba = ba_char 65536
  let zd = Zlib.create_inflate ~window_bits:(47) ()
  let cache_pbf_from_blob t blob =
    let blob_size = (String.length blob) in
    zd.in_ba  <- zlib_in_ba ;
    zd.out_ba <- zlib_out_ba;
    for i=0 to (blob_size-1) do zd.in_ba.{i}<-(String.get blob i) done;
    (match (Zlib.flate zd Zlib.Finish) with
    | Zlib.Stream_end -> () (* can be Ok, Need_dict, Buf_error, Data_error *)
    | _ -> raise UnhandledZlibIssue);
    if (zd.in_ofs != blob_size) then Printf.printf "Expected to decompress %d bytes but decompressed %d\n" blob_size zd.in_ofs;
    let pbf_size = zd.out_ofs in
    let pbf_data = ba_char pbf_size in
    Bigarray.Array1.(blit (sub zd.out_ba 0 pbf_size) pbf_data);
    t.cached_pbf <- Some pbf_data

  (*f All done *)

end

(*a TileSet *)
(*m TileSet module *)
module TileSet = struct
  (*t t structure *)
  type t = {
      zoom: int;
      mutable tiles_of_xy : ((int*int) * Tile.t) list;
    }

  (*f create zoom:int *)
  let create zoom =
    { zoom;
      tiles_of_xy=[];
    }

  (*f add_tile t x:int y:int s:string *)
  let add_tile t x y s =
    t.tiles_of_xy <- ((x,y), (Tile.create x y s))::t.tiles_of_xy

  (*f tiles_within_bbox ?bbox t - find tiles within optional bbox *)
  let tiles_within_bbox ?bbox t =
    let filter =
      match bbox with
      | None -> fun _ -> true
      | Some bbox -> Tile.tile_within_bbox bbox
    in
    let add_if_true f acc xy_tile =
      let (_, tile) = xy_tile in
      if (filter tile) then (tile::acc) else acc
    in
    List.fold_left (add_if_true filter) [] t.tiles_of_xy

  (*f get_tile_opt *)
  let get_tile_opt t x y =
    List.assoc_opt (x,y) t.tiles_of_xy

  (*f All done *)
end

