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
 * @file      maptile_vector_pbf.ml
 * @brief     Decode PBF from maptile as vectors
 *
 *)

open Batteries
open Mbtiles
module Tile     = Vector.Tile
module Layer    = Vector.Layer
module Feature  = Vector.Feature
module Value    = Vector.Value
module KeyValue = Vector.KeyValue
module Geometry = Vector.Geometry

(*a Top level *)
let map = File.create "/Users/gavinprivate/Git/brew/map/2017-07-03_england_cambridgeshire.mbtiles"

let play_with_feature tile layer feature =
  let uid = (Feature.uid feature) in
  let geom = (Tile.feature_geometry layer feature) in
  Geometry.display geom;
  if uid<5 then (
    Printf.printf "Feature uid %d (%d kv)\n" uid (Tile.feature_kv_count layer feature);
    let print_name kv =
      if (KeyValue.key_equals kv "name") then (
        let (ks,vs)=KeyValue.strs kv in
        Printf.printf "  %s->%s\n" ks vs
      )
    in
    Tile.feature_kv_iter layer feature print_name;
    Tile.feature_kv_map_default layer feature (fun v->Printf.printf "rank %d\n" (Value.as_int v)) () "rank";
  );
  ()
    

let _ =
  File.read_all_tiles map;
  File.display map;    
  let opt_t = File.get_tile_opt map 0 0 0 in
  let opt_t = File.get_tile_opt map 6 32 43 in (* 0 0 0 *)
  let opt_t = File.get_tile_opt map 9 256 344 in (* 0 0 0 *)
  let opt_t = File.get_tile_opt map 11 1025 1376 in (* 0 0 0 *)
  let opt_t = File.get_tile_opt map 14 8170 (8*1376) in (* 0 0 0 *)
  let t = Option.get opt_t in
  let pbf = File.get_tile_pbf map t in
  (*let tile = Tile.create ~layer_filter_fn:(String.equal "place") () in*)
  let tile = Tile.create () in
  Tile.parse_pbf tile pbf;
  let layer = Option.get (Tile.get_layer tile "building") in
  Layer.display layer;
  Layer.display ~all:true layer;
  Tile.layer_iter tile (fun l -> Printf.printf "Layer %s has %d features\n" (Layer.name l) (Tile.feature_count l));
  Printf.printf "Estimated size in bytes %d\n" (Tile.byte_size tile);
  Tile.feature_iter layer (play_with_feature tile layer);
  File.close map
