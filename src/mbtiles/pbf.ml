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
 * @file      pbf.ml
 * @brief     Decode PBF
 *)

(*a Types *)
type t_ba_char   = (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t
type t_pbf = t_ba_char

(*t t_pbf_wire - internal 'on the wire' data *)
type t_pbf_wire = 
  | Value of int64
  | Blob of (int * int) (* offset, length *)

(*f Exception BadProtobuf *)
exception BadProtobuf
exception BadProtobufFn

(*a Useful functions *)
(*f sfmt *)
let sfmt = Printf.sprintf

(*a Reading functions *)
(*f read_varint - read a varint from a byte bigarray at an offset as int64 and return new offset *)
let read_varint b o =
  let rec acc_bits i acc =
    let v = Char.code b.{o+i} in
    let value = v land 0x7f in
    let continues = (v land 0x80)!=0 in
    let new_acc = Int64.(logor (shift_left (of_int value) (7*i))acc) in
    if (continues) then (acc_bits (i+1) new_acc) else (i+1, new_acc)
    in
  let (n, value) = acc_bits 0 0L in
  (value, o+n)

(*f read_int_n - read an N byte integer (as an int64) little endian *)
let read_int_n b o n =
  let rec acc_int n acc =
  if (n<0) then acc else (
    acc_int (n-1) Int64.(logor (shift_left acc 8) (of_int (Char.code b.{o+n})))
  ) in
  ((acc_int (n-1) 0L), o+n)

(*f read_int_of_4 - read a 4 byte integer (as an int64) little endian *)
let read_int_of_4 b o = read_int_n b o 4

(*f read_int_of_8 - read an 8 byte integer (as an int64) little endian *)
let read_int_of_8 b o = read_int_n b o 8

(*f read_key - read a protobuf key as field number/type (from a varint) *)
let read_key b o =
    let (k, no) = read_varint b o in
    let field_number = Int64.(shift_right k 3) in
    let field_type   = (Int64.to_int k) land 7 in
    (field_number, field_type, no)

(*f read_key_value - read a key/value pair and return key, wire value and new offset *)
let read_key_value b o =
    let (field_key, field_type, no) = read_key b o in
    match field_type with
    | 0 -> let (value, no) = read_varint   b no in (field_key, Value value, no)
    | 1 -> let (value, no) = read_int_of_8 b no in (field_key, Value value, no)
    | 5 -> let (value, no) = read_int_of_4 b no in (field_key, Value value, no)
    | 2 -> let (size64, no) = read_varint   b no in
           let size = Int64.to_int size64 in
           (field_key, Blob (no,size), no+size)
    | _ -> raise BadProtobuf
       
(*f zigzag_signed - decode a signed integer *)
let zigzag_signed x =
  let magnitude = Int64.shift_right_logical x 1 in
  let negative = (Int64.logand x 1L)==1L in
  if negative then (Int64.logxor magnitude 0xffffffffffffffffL) else magnitude

(*a Module Value *)
let double_of_int64 v = Int64.float_of_bits v
let float_of_int32 v  = Int32.float_of_bits v
module Value = struct
  (*t BadValue exception *)
  exception BadValue

  (*f ofs_len *)
  let ofs_len (w:t_pbf_wire) =
    match w with
    | Blob (ofs,len) -> (ofs,len)
    | _ -> raise BadValue

  (*f int64 *)
  let int64 (w:t_pbf_wire) =
    match w with
    | Value v -> v
    | _ -> raise BadValue

  (*f sint64 *)
  let sint64 w =
    zigzag_signed (int64 w)

  (*f uint64 *)
  let uint64 w = int64 w

  (*f double *)
  let double w =
     double_of_int64 (int64 w)

  (*f sint32 *)
  let sint32 w =
    Int64.to_int32 (zigzag_signed (int64 w))

  (*f int32 *)
  let int32 w =
    Int64.to_int32 (int64 w)

  (*f uint32 *)
  let uint32 w = int32 w

  (*f int *)
  let int w =
    Int64.to_int (int64 w)

  (*f bool *)
  let bool w =
    (int64 w) == 1L

  (*f float *)
  let float w =
     float_of_int32 (int32 w)

  (*f string *)
  let string b (w:t_pbf_wire) =
    match w with
    | Blob (ofs,len) -> String.init len (fun i -> b.{ofs+i})
    | _ -> raise BadValue

  (*f rep_fixed64 *)
  let rep_fixed64 b (w:t_pbf_wire) =
    match w with
    | Value v -> Array.make 1 v
    | Blob (ofs, len) ->
       if ((len mod 8)!=0) then (
         raise BadValue
       ) else (
         Array.init (len/8) (fun i -> let (v,n)=read_int_of_8 b (ofs+8*i) in v)
       )

  (*f rep_sfixed64 *)
  let rep_sfixed64 b (w:t_pbf_wire) =
    match w with
    | Value v -> Array.make 1 (zigzag_signed v)
    | Blob (ofs, len) ->
       if ((len mod 8)!=0) then (
         raise BadValue
       ) else (
         Array.init (len/8) (fun i -> let (v,n)=read_int_of_8 b (ofs+8*i) in zigzag_signed v)
       )

  (*f rep_double *)
  let rep_double b (w:t_pbf_wire) =
    match w with
    | Value v -> Array.make 1 (double_of_int64 v)
    | Blob (ofs, len) ->
       if ((len mod 8)!=0) then (
         raise BadValue
       ) else (
         Array.init (len/8) (fun i -> let (v,n)=read_int_of_8 b (ofs+8*i) in (double_of_int64 v))
       )

  (*f rep_fixed32 *)
  let rep_fixed32 b (w:t_pbf_wire) =
    match w with
    | Value v -> Array.make 1 (Int64.to_int32 v)
    | Blob (ofs, len) ->
       if ((len mod 4)!=0) then (
         raise BadValue
       ) else (
         Array.init (len/4) (fun i -> let (v,n)=read_int_of_4 b (ofs+4*i) in (Int64.to_int32 v))
       )

  (*f rep_sfixed32 *)
  let rep_sfixed32 b (w:t_pbf_wire) =
    match w with
    | Value v -> Array.make 1 (Int64.to_int32 (zigzag_signed v))
    | Blob (ofs, len) ->
       if ((len mod 4)!=0) then (
         raise BadValue
       ) else (
         Array.init (len/4) (fun i -> let (v,n)=read_int_of_4 b (ofs+4*i) in (Int64.to_int32 (zigzag_signed v)))
       )
  (*f rep_float *)
  let rep_float b (w:t_pbf_wire) =
    match w with
    | Value v -> Array.make 1 (float_of_int32 (Int64.to_int32 v))
    | Blob (ofs, len) ->
       if ((len mod 4)!=0) then (
         raise BadValue
       ) else (
         Array.init (len/4) (fun i -> let (v,n)=read_int_of_4 b (ofs+4*i) in (float_of_int32 (Int64.to_int32 v)))
       )

  (*f array_of_varints *)
  let array_of_varints b ofs len f =
    let next_block = ofs+len in
    let rec acc_varint (n,acc) o =
      if (o>=next_block) then (n,acc) else (
        let (v,no) = read_varint b o in
        let new_acc = v::acc in
        acc_varint (n+1,new_acc) no 
      )
    in
    let (n,l) = acc_varint (0,[]) ofs in
    let a = Array.make n (f 0L) in
    List.iteri (fun i v -> a.(n-1-i)<-f v;) l;
    a

  (*f rep_varint *)
  let rep_varint b (w:t_pbf_wire) f =
    match w with
    | Value v -> Array.make 1 (f v)
    | Blob (ofs, len) -> array_of_varints b ofs len f

  (*f rep_uint64 *)
  let rep_uint64 b (w:t_pbf_wire) =
    rep_varint b w (fun v -> v)

  (*f rep_sint64 *)
  let rep_sint64 b (w:t_pbf_wire) =
    rep_varint b w zigzag_signed

  (*f rep_int64 *)
  let rep_int64 b (w:t_pbf_wire) =
    rep_varint b w (fun v->v)

  (*f rep_uint32 *)
  let rep_uint32 b (w:t_pbf_wire) =
    rep_varint b w Int64.to_int32

  (*f rep_sint32 *)
  let rep_sint32 b (w:t_pbf_wire) =
    rep_varint b w (fun v -> Int64.to_int32 (zigzag_signed v))

  (*f rep_int32 *)
  let rep_int32 b (w:t_pbf_wire) =
    rep_varint b w Int64.to_int32

  (*f rep_int *)
  let rep_int b (w:t_pbf_wire) =
    rep_varint b w Int64.to_int

  (*f rep_sint *)
  let rep_sint b (w:t_pbf_wire) =
    rep_varint b w (fun v->Int64.to_int (zigzag_signed v))

  (*f rep_bool *)
  let rep_bool b (w:t_pbf_wire) =
    rep_varint b w (fun v->v==1L)

  (*f str *)
  let str v =
    match v with
    | Value  f -> sfmt "%Ld" f
    | Blob (ofs,len) -> sfmt "blob(%d,%d)" ofs len

  (*f value_str not used
let value_str v =
    match v with
  | Float  f -> sfmt "%g" f
  | Bool   b -> sfmt "%b" b
  | Enum   e -> sfmt "%Ld" e
  | Sint32 i32 -> sfmt "%ld" i32
  | Uint32 i32 -> sfmt "%ld" i32
  | Sint64 i64 -> sfmt "%Ld" i64
  | Uint64 i64 -> sfmt "%Ld" i64
  | Blob (ofs,len) -> sfmt "blob(%d,%d)" ofs len
 *)
end


(*a Parsing *)
(*t t_pbf_fold_fn *)
type ('a,'b) t_pbf_gen_fold_fn = 'a -> 'b -> 'a
type 'a t_pbf_fold_fn =
  | PbfDouble of ('a, float)   t_pbf_gen_fold_fn
  | PbfFloat  of ('a, float)   t_pbf_gen_fold_fn
  | PbfBool   of ('a, bool)    t_pbf_gen_fold_fn
  | PbfEnum   of ('a, int)     t_pbf_gen_fold_fn
  | PbfInt    of ('a, int)     t_pbf_gen_fold_fn
  | PbfString of ('a, string)  t_pbf_gen_fold_fn
  | PbfSint32 of ('a, int32)   t_pbf_gen_fold_fn
  | PbfUint32 of ('a, int32)   t_pbf_gen_fold_fn
  | PbfSint64 of ('a, int64)   t_pbf_gen_fold_fn
  | PbfUint64 of ('a, int64)   t_pbf_gen_fold_fn
  | PbfRepFixed64  of ('a, int64 array) t_pbf_gen_fold_fn
  | PbfRepSfixed64 of ('a, int64 array) t_pbf_gen_fold_fn
  | PbfRepDouble   of ('a, float array) t_pbf_gen_fold_fn
  | PbfRepFixed32  of ('a, int32 array) t_pbf_gen_fold_fn
  | PbfRepSfixed32 of ('a, int32 array) t_pbf_gen_fold_fn
  | PbfRepFloat  of ('a, float array) t_pbf_gen_fold_fn
  | PbfRepUint64 of ('a, int64 array) t_pbf_gen_fold_fn
  | PbfRepSint64 of ('a, int64 array) t_pbf_gen_fold_fn
  | PbfRepInt64  of ('a, int64 array) t_pbf_gen_fold_fn
  | PbfRepUint32 of ('a, int32 array) t_pbf_gen_fold_fn
  | PbfRepSint32 of ('a, int32 array) t_pbf_gen_fold_fn
  | PbfRepInt32  of ('a, int32 array) t_pbf_gen_fold_fn
  | PbfRepInt    of ('a, int array) t_pbf_gen_fold_fn
  | PbfRepSint   of ('a, int array) t_pbf_gen_fold_fn
  | PbfRepBool   of ('a, bool array) t_pbf_gen_fold_fn
  | PbfBlob   of ('a, (t_pbf *int * int)) t_pbf_gen_fold_fn

(*f exec_fold_fn - execute the correct kind of fold function *)
let exec_fold_fn pbf f acc v =
    match f with
    | PbfDouble f   -> f acc (Value.double v)
    | PbfFloat  f   -> f acc (Value.float  v)
    | PbfBool  f    -> f acc (Value.bool  v)
    | PbfEnum  f    -> f acc (Value.int  v)
    | PbfInt   f    -> f acc (Value.int  v)
    | PbfString  f  -> f acc (Value.string  pbf v)
    | PbfSint32  f  -> f acc (Value.sint32  v)
    | PbfSint64  f  -> f acc (Value.sint64  v)
    | PbfUint32  f  -> f acc (Value.uint32  v)
    | PbfUint64  f  -> f acc (Value.uint64  v)
    | PbfRepFixed64   f  -> f acc (Value.rep_fixed64 pbf v)
    | PbfRepSfixed64  f  -> f acc (Value.rep_sfixed64 pbf v)
    | PbfRepDouble    f  -> f acc (Value.rep_double pbf v)
    | PbfRepFixed32   f  -> f acc (Value.rep_fixed32 pbf v)
    | PbfRepSfixed32  f  -> f acc (Value.rep_sfixed32 pbf v)
    | PbfRepFloat     f  -> f acc (Value.rep_float pbf v)
    | PbfRepUint64    f  -> f acc (Value.rep_uint64 pbf v)
    | PbfRepSint64    f  -> f acc (Value.rep_sint64 pbf v)
    | PbfRepInt64     f  -> f acc (Value.rep_int64 pbf v)
    | PbfRepUint32    f  -> f acc (Value.rep_uint32 pbf v)
    | PbfRepSint32    f  -> f acc (Value.rep_sint32 pbf v)
    | PbfRepInt32     f  -> f acc (Value.rep_int32 pbf v)
    | PbfRepInt       f  -> f acc (Value.rep_int pbf v)
    | PbfRepSint      f  -> f acc (Value.rep_sint pbf v)
    | PbfRepBool      f  -> f acc (Value.rep_bool  pbf v)
    | PbfBlob    f  -> let (ofs,len) = (Value.ofs_len v) in f acc (pbf, ofs, len)

(*f fold_message - Fold over a complete protobuf message, invoking appropriate function for each element *)
let fold_message fns acc pbf ofs len =
  let next_block = ofs+len in
  let rec fold_over_kv acc ofs =
    if (ofs>=next_block) then (
      acc
    ) else (
      let (k,v,new_ofs) = read_key_value pbf ofs in
      let new_acc = 
        if (List.mem_assoc k fns) then (
          exec_fold_fn pbf (List.assoc k fns) acc v
        ) else (
          acc
        )
      in
      fold_over_kv new_acc new_ofs
    )
  in
  fold_over_kv acc ofs


