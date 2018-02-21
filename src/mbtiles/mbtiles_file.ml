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
 * @file      mbtiles_file.ml
 * @brief     Provide access to OpenStreetMap/MapBox .mbtile data files    
 *
 * .mbtile files are sqlite3 databases
 *
 * mbtiles sqlite database has the following tables:
 * 
 * . images (tile id -> blob)
 * 
 * . map (zoom_level:int -> tile_column:int -> tile_row:int -> tile_id:string)
 * 
 * . metadata (name:string -> value:string)
 * 
 * . gpkg_spatial_ref_sys (srs_name:string -> srs_id:int key -> organization:string -> org_id:int -> definition:string -> description:string )
 * 
 * . gpkg_contents (table_name:string key -> data_type:string -> identifiter:string unique -> last_change:date_time -> min_x:float -> min_y:float -> max_x:float -> max_y:float -> srs_id:string
 * 
 * An example:
 * gpkg_contents:
 *    "package_tiles" "tiles" "package_tiles" "TileShrink GL" "2017-08-01T00:33:07.000Z" (-55896.3883701651212) 6801060.31069149077 57462.1192720706749 6935334.02115962468 900913L
 * 
 * gpkg_spatial_ref_sys:
 * [|
 * "Undefined Cartesian" -1L "NONE" -1L "Undefined" NULL;
 * "Undefined Geographic" 0L "NONE" 0L "Undefined" NULL;
 * "WGS 84 / Pseudo-Mercator"  3857L "epsg" 3857L "PROJCS[\"WGS84...\"]" NULL;
 * "WGS 84"  4326L "epsg" 4236L "GEOGCS[\"WGS 84\"...]" NULL;
 * "Google Maps Global Mercator"  900913L "epsg" 900913L  "PROJCS[\"Google...\"]" NULL;
 * |]
 * 
 * metadata:
 * [|
 * "attribution"   "<a href=\"http://www.openmaptiles.org/\" target=\"_blank\">&copy; OpenMapTiles</a> <a href=\"http://www.openstreetmap.org/about/\" target=\"_blank\">&copy; OpenStreetMap contributors</a>"
 * "center"        "0.0070326,52.37336,14"
 * "description"   "Extract from https://openmaptiles.org"
 * "maxzoom"       "14"
 * "minzoom"       "0"
 * "name"          "OpenMapTiles"
 * "pixel_scale"   "256"
 * "mtime"         "1499626373833"
 * "format"        "pbf"
 * "id"            "openmaptiles"
 * "version"       "3.6.1"
 * "maskLevel"     "5"
 * "bounds"        "-0.5021258,52.00517,0.516191,52.74155"
 * "planettime"    "1499040000000"
 * "json"          "{\"vector_layers\":[{\"maxzoom\":14,\"fields\":{\"class\":\"String\"},\"minzoom\":0,\"id\":\"water\",\"description\":\"\"},{\"maxzoom\":14,\"fields\":{\"name:mt\":\"String\",\"name:pt\":\"String\",\"name:az\":\"String\",\"name:ka\":\"String\",\"name:rm\":\"String\",\"name:ko\":\"String\",\"name:kn\":\"String\",\"name:ar\":\"String\",\"name:cs\":\"Strin"... }
 * "basename"      "england_cambridgeshire.mbtiles"
 * |]
 * 
 * map:
 * 14L   8169L 10972L  "14/8169/10972"
 * 14L   8169L 10973L  "14/8169/10973"
 * 14L   8169L 10974L  "14/8169/10974"
 * ...
 * 
 * images:
 * "14/8169/10972" (BLOB "\031\213\008\000\000\000\000\000\000\003...")
 * "14/8169/10973" (BLOB "\031\213\008\000\000\000\000\000\000\003...")
 * ...
 * 
 * The BLOBs are zlib-compressed PBF (Google protocol buffer) messages

let s = Sqlite3.prepare db "SELECT * FROM sqlite_master"

 *)

(*a Libraries - Batteries for Option *)
open Batteries
module TileSet = Mbtiles_tiles.TileSet
module Tile    = Mbtiles_tiles.Tile

(*a Useful functions *)
(*f ba_char *)
type t_ba_char = (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t
let ba_char size = Bigarray.(Array1.create char c_layout size)

(*f sfmt *)
let sfmt = Printf.sprintf

(*f *_of_sql_data *)
exception BadMbtileSql
exception MismatchInSqlData of string
let int_of_sql_data d =
    match d with 
    | Sqlite3.Data.INT x -> (Int64.to_int x)
    | _ -> raise (MismatchInSqlData (Sqlite3.Data.to_string_debug d))

let float_of_sql_data d =
    match d with 
    | Sqlite3.Data.FLOAT x -> x
    | _ -> raise (MismatchInSqlData (Sqlite3.Data.to_string_debug d))

let string_of_sql_data d =
    match d with 
    | Sqlite3.Data.TEXT x -> x
    | _ -> raise (MismatchInSqlData (Sqlite3.Data.to_string_debug d))

let blob_of_sql_data d =
    match d with 
    | Sqlite3.Data.BLOB x -> x
    | _ -> raise (MismatchInSqlData (Sqlite3.Data.to_string_debug d))

(*f fold/iter sql_row *)
let rec fold_sql_row f acc s =
  match (Sqlite3.step s) with
  | Sqlite3.Rc.ROW -> fold_sql_row f (f acc (Sqlite3.row_data s)) s
  | _ -> acc

let rec iter_sql_row f s =
  match (Sqlite3.step s) with
  | Sqlite3.Rc.ROW -> (
    f (Sqlite3.row_data s);
    iter_sql_row f s
  )
  | _ -> ()

(*a File module *)
(*m File module *)
module File = struct

  (*t t structure *)
  type t = {
      filename: string;
      db: Sqlite3.db;
      zoom_levels: int list;
      map_tiles_by_zoom : (int * TileSet.t) list;
    }

  (*f zoom_levels getter *)
  let zoom_levels t = t.zoom_levels

  (*f get_zoom_levels db - read zoom levels from sql database *)
  let get_zoom_levels db =
    let s = Sqlite3.prepare db "SELECT DISTINCT zoom_level FROM map" in
    fold_sql_row (fun acc r -> (int_of_sql_data r.(0))::acc) [] s

  (*f create filename:string *)
  let create filename =
    let db = Sqlite3.db_open ~mode:`READONLY filename in
    let zoom_levels = get_zoom_levels db in
    let t = {
        filename; db; zoom_levels;
        map_tiles_by_zoom = List.map (fun z->(z,TileSet.create z)) zoom_levels
      } in
    t

  (*f close t - close the database *)
  let close t =
    Sqlite3.db_close t.db

  (*f tileset_of_zoom t zoom_level:int - get the tileset for a zoom level *)
  let tileset_of_zoom t zoom_level =
    List.assoc zoom_level t.map_tiles_by_zoom

  (*f read_tiles t zoom_level:int - read the tiles into a tileset for a zoom level - note this does not read/cache the contents *)
  let read_tiles t zoom_level =
    let s = Sqlite3.prepare t.db (sfmt "SELECT tile_column,tile_row,tile_id FROM map WHERE zoom_level=%d" zoom_level) in
    let add_tile tileset r =
      let x = int_of_sql_data r.(0) in
      let y = int_of_sql_data r.(1) in
      let s = string_of_sql_data r.(2) in
      TileSet.add_tile tileset x y s
    in
    iter_sql_row (add_tile (tileset_of_zoom t zoom_level)) s

  (*f read_all_tiles t - reads all the tilesets for all zoom levels *)
  let read_all_tiles t =
    List.iter (read_tiles t) t.zoom_levels

  (*f tiles_of_zoom_level ?bbox t zoom_levels - return a list of tiles within a zoom level within an optional bbox *)
  let tiles_of_zoom_level ?bbox t zoom_level =
    if (List.mem zoom_level t.zoom_levels) then (
      TileSet.tiles_within_bbox ?bbox:bbox (tileset_of_zoom t zoom_level)
    ) else (
      []
    )

  (*f get_tile_opt *)
  let get_tile_opt t zoom_level x y =
    TileSet.get_tile_opt (tileset_of_zoom t zoom_level) x y

  (*f get_tile_pbf *)
  let get_tile_pbf t tile =
    if (not (Tile.pbf_valid tile)) then (
      let tile_name = Tile.get_name tile in
      let s = Sqlite3.prepare t.db (sfmt "SELECT tile_data FROM images WHERE tile_id='%s' LIMIT 1" tile_name) in
      match (Sqlite3.step s) with
      | Sqlite3.Rc.ROW -> (
        let row = Sqlite3.row_data s in
        Tile.cache_pbf_from_blob tile (blob_of_sql_data row.(0))
      )
      | _ -> raise BadMbtileSql
    );
    Tile.get_pbf tile

  (*f display - show stuff *)
  let display ?max_tiles:(max_tiles=10) t =
    let display_zoom z =
      let tiles = tiles_of_zoom_level t z in
      let num_tiles = List.length tiles
      in
      Printf.printf "Zoom level %d has %d tiles\n" z num_tiles;
      let tiles_to_display = min max_tiles num_tiles in
      let indent="    " in
      for i=0 to tiles_to_display-1 do
        let tile = (List.nth tiles i) in
        let (x,y) = Tile.get_xy tile in
        let name = Tile.get_name tile in
        Printf.printf "%sTile %s @ (%d,%d)\n" indent name x y;
      done;
      if (tiles_to_display!=num_tiles) then
        Printf.printf"%s...\n" indent;
      ()
    in
    List.iter display_zoom t.zoom_levels

  (*f All done *)
end
    



