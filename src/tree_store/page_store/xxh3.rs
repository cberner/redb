// Copyright (c) 2022 Christopher Berner
//
// MIT License
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// Copied from xxh3 crate, commit hash f5a89eb888490f378cb7b4155d905bbfda9fc5f9

// Unsafe code in this module is limited to:
// * the load & store helpers of each SIMD backend, which take references to arrays of exactly
//   the size the intrinsic reads or writes
// * calls to the target_feature functions of each SIMD backend, which happen only after
//   detecting that the CPU supports those features
//
// Everything else, including all slicing of the input and secret, is safe code

use core::mem::size_of;

// The NEON code loads bytes into its 64-bit lanes in the target's byte order, which is the
// little-endian order xxh3 is defined in only on a little-endian target
#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
use core::arch::aarch64::*;
#[cfg(target_arch = "x86")]
use core::arch::x86::*;
#[cfg(target_arch = "x86_64")]
use core::arch::x86_64::*;

// Whether the CPU supports a target feature. Detecting that at runtime needs std, so without it,
// only the features the target enables at compile time are used
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
macro_rules! x86_feature_detected {
    ($feature:tt) => {{
        #[cfg(not(redb_no_std))]
        {
            std::arch::is_x86_feature_detected!($feature)
        }
        #[cfg(redb_no_std)]
        {
            cfg!(target_feature = $feature)
        }
    }};
}

#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
macro_rules! aarch64_feature_detected {
    ($feature:tt) => {{
        #[cfg(not(redb_no_std))]
        {
            std::arch::is_aarch64_feature_detected!($feature)
        }
        #[cfg(redb_no_std)]
        {
            cfg!(target_feature = $feature)
        }
    }};
}

const STRIPE_LENGTH: usize = 64;
const SECRET_CONSUME_RATE: usize = 8;

const MIN_SECRET_SIZE: usize = 136;
const DEFAULT_SECRET: [u8; 192] = [
    0xb8, 0xfe, 0x6c, 0x39, 0x23, 0xa4, 0x4b, 0xbe, 0x7c, 0x01, 0x81, 0x2c, 0xf7, 0x21, 0xad, 0x1c,
    0xde, 0xd4, 0x6d, 0xe9, 0x83, 0x90, 0x97, 0xdb, 0x72, 0x40, 0xa4, 0xa4, 0xb7, 0xb3, 0x67, 0x1f,
    0xcb, 0x79, 0xe6, 0x4e, 0xcc, 0xc0, 0xe5, 0x78, 0x82, 0x5a, 0xd0, 0x7d, 0xcc, 0xff, 0x72, 0x21,
    0xb8, 0x08, 0x46, 0x74, 0xf7, 0x43, 0x24, 0x8e, 0xe0, 0x35, 0x90, 0xe6, 0x81, 0x3a, 0x26, 0x4c,
    0x3c, 0x28, 0x52, 0xbb, 0x91, 0xc3, 0x00, 0xcb, 0x88, 0xd0, 0x65, 0x8b, 0x1b, 0x53, 0x2e, 0xa3,
    0x71, 0x64, 0x48, 0x97, 0xa2, 0x0d, 0xf9, 0x4e, 0x38, 0x19, 0xef, 0x46, 0xa9, 0xde, 0xac, 0xd8,
    0xa8, 0xfa, 0x76, 0x3f, 0xe3, 0x9c, 0x34, 0x3f, 0xf9, 0xdc, 0xbb, 0xc7, 0xc7, 0x0b, 0x4f, 0x1d,
    0x8a, 0x51, 0xe0, 0x4b, 0xcd, 0xb4, 0x59, 0x31, 0xc8, 0x9f, 0x7e, 0xc9, 0xd9, 0x78, 0x73, 0x64,
    0xea, 0xc5, 0xac, 0x83, 0x34, 0xd3, 0xeb, 0xc3, 0xc5, 0x81, 0xa0, 0xff, 0xfa, 0x13, 0x63, 0xeb,
    0x17, 0x0d, 0xdd, 0x51, 0xb7, 0xf0, 0xda, 0x49, 0xd3, 0x16, 0x55, 0x26, 0x29, 0xd4, 0x68, 0x9e,
    0x2b, 0x16, 0xbe, 0x58, 0x7d, 0x47, 0xa1, 0xfc, 0x8f, 0xf8, 0xb8, 0xd1, 0x7a, 0xd0, 0x31, 0xce,
    0x45, 0xcb, 0x3a, 0x8f, 0x95, 0x16, 0x04, 0x28, 0xaf, 0xd7, 0xfb, 0xca, 0xbb, 0x4b, 0x40, 0x7e,
];

const PRIME32: [u64; 3] = [0x9E3779B1, 0x85EBCA77, 0xC2B2AE3D];
const PRIME64: [u64; 5] = [
    0x9E3779B185EBCA87,
    0xC2B2AE3D27D4EB4F,
    0x165667B19E3779F9,
    0x85EBCA77C2B2AE63,
    0x27D4EB2F165667C5,
];

const INIT_ACCUMULATORS: [u64; 8] = [
    PRIME32[2], PRIME64[0], PRIME64[1], PRIME64[2], PRIME64[3], PRIME32[1], PRIME64[4], PRIME32[0],
];

// The STRIPE_LENGTH bytes of the secret starting at offset
fn secret_window(secret: &[u8], offset: usize) -> &[u8; STRIPE_LENGTH] {
    secret[offset..].first_chunk().unwrap()
}

macro_rules! accumulate_block {
    ($acc:expr, $stripes:expr, $secret:expr, $accum_stripe:expr) => {{
        for (i, stripe) in $stripes.iter().enumerate() {
            $accum_stripe(
                $acc,
                stripe,
                secret_window($secret, i * SECRET_CONSUME_RATE),
            );
        }
    }};
}

macro_rules! hash_large_helper {
    ($data:expr, $secret:expr, $scramble:expr, $accum_stripe:expr) => {{
        let data: &[u8] = $data;
        let secret: &[u8] = $secret;
        let mut accumulators = INIT_ACCUMULATORS;

        let stripes_per_block = (secret.len() - STRIPE_LENGTH) / SECRET_CONSUME_RATE;
        let block_len = STRIPE_LENGTH * stripes_per_block;
        let blocks = (data.len() - 1) / block_len;

        // The last stripe is always the final STRIPE_LENGTH bytes of the input, handled below, so
        // the stripes accumulated before it never include the input's last byte
        let (stripes, _) = data[..data.len() - 1].as_chunks::<STRIPE_LENGTH>();
        let (full_blocks, last_block) = stripes.split_at(blocks * stripes_per_block);
        for block in full_blocks.chunks_exact(stripes_per_block) {
            accumulate_block!(&mut accumulators, block, secret, $accum_stripe);
            $scramble(&mut accumulators, secret.last_chunk().unwrap());
        }
        accumulate_block!(&mut accumulators, last_block, secret, $accum_stripe);

        $accum_stripe(
            &mut accumulators,
            data.last_chunk().unwrap(),
            secret_window(secret, secret.len() - STRIPE_LENGTH - 7),
        );

        accumulators
    }};
}

macro_rules! hash64_large_impl {
    ($data:expr, $seed:expr, $generate:expr, $scramble:expr, $accum_stripe:expr) => {{
        let secret = $generate($seed);
        let accumulators = hash_large_helper!($data, &secret, $scramble, $accum_stripe);

        merge_accumulators(
            accumulators,
            &secret[11..],
            PRIME64[0].wrapping_mul($data.len() as u64),
        )
    }};
}

macro_rules! hash128_large_impl {
    ($data:expr, $seed:expr, $generate:expr, $scramble:expr, $accum_stripe:expr) => {{
        let secret = $generate($seed);
        let accumulators = hash_large_helper!($data, &secret, $scramble, $accum_stripe);

        let low = merge_accumulators(
            accumulators,
            &secret[11..],
            PRIME64[0].wrapping_mul($data.len() as u64),
        );
        let high = merge_accumulators(
            accumulators,
            &secret[secret.len() - 64 - 11..],
            !(PRIME64[1].wrapping_mul($data.len() as u64)),
        );

        ((u128::from(high)) << 64) | u128::from(low)
    }};
}

#[must_use]
pub fn hash64_with_seed(data: &[u8], seed: u64) -> u64 {
    if data.len() <= 240 {
        hash64_0to240(data, &DEFAULT_SECRET, seed)
    } else {
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            if x86_feature_detected!("avx512f") {
                // SAFETY: the CPU supports AVX-512F, the only feature hash64_large_avx512 enables
                return unsafe { hash64_large_avx512(data, seed) };
            }
            if x86_feature_detected!("avx2") {
                // SAFETY: the CPU supports AVX2, the only feature hash64_large_avx2 enables
                return unsafe { hash64_large_avx2(data, seed) };
            }
        }
        #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
        {
            if aarch64_feature_detected!("neon") {
                // SAFETY: the CPU supports NEON, the only feature hash64_large_neon enables
                return unsafe { hash64_large_neon(data, seed) };
            }
        }
        hash64_large_generic(data, seed)
    }
}

#[must_use]
pub fn hash128_with_seed(data: &[u8], seed: u64) -> u128 {
    if data.len() <= 240 {
        hash128_0to240(data, &DEFAULT_SECRET, seed)
    } else {
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            if x86_feature_detected!("avx512f") {
                // SAFETY: the CPU supports AVX-512F, the only feature hash128_large_avx512 enables
                return unsafe { hash128_large_avx512(data, seed) };
            }
            if x86_feature_detected!("avx2") {
                // SAFETY: the CPU supports AVX2, the only feature hash128_large_avx2 enables
                return unsafe { hash128_large_avx2(data, seed) };
            }
        }
        #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
        {
            if aarch64_feature_detected!("neon") {
                // SAFETY: the CPU supports NEON, the only feature hash128_large_neon enables
                return unsafe { hash128_large_neon(data, seed) };
            }
        }
        hash128_large_generic(data, seed)
    }
}

fn get_u32(data: &[u8], i: usize) -> u32 {
    u32::from_le_bytes(
        data[i * size_of::<u32>()..(i + 1) * size_of::<u32>()]
            .try_into()
            .unwrap(),
    )
}

fn get_u64(data: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(
        data[i * size_of::<u64>()..(i + 1) * size_of::<u64>()]
            .try_into()
            .unwrap(),
    )
}

fn xxh64_avalanche(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(PRIME64[1]);
    x ^= x >> 29;
    x = x.wrapping_mul(PRIME64[2]);
    x ^= x >> 32;
    x
}

fn xxh3_avalanche(mut x: u64) -> u64 {
    x = xorshift(x, 37);
    x = x.wrapping_mul(0x165667919E3779F9);
    x = xorshift(x, 32);
    x
}

fn merge_accumulators(
    accumulators: [u64; INIT_ACCUMULATORS.len()],
    secret: &[u8],
    init: u64,
) -> u64 {
    let mut result = init;
    for i in 0..4 {
        let a1 = accumulators[2 * i];
        let a2 = accumulators[2 * i + 1];
        let s1 = get_u64(&secret[16 * i..], 0);
        let s2 = get_u64(&secret[16 * i..], 1);
        result = result.wrapping_add(mul128_and_xor(a1 ^ s1, a2 ^ s2));
    }
    xxh3_avalanche(result)
}

// Load & store helpers for the SIMD backends. They're the only code in those backends that needs
// an unsafe block. Each one takes a reference to an array of exactly the size the intrinsic reads
// or writes, and the array type is aligned at least as much as the intrinsic requires

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
fn load_bytes_avx2(bytes: &[u8; 32]) -> __m256i {
    // SAFETY: bytes is valid for a 32 byte read, and loadu has no alignment requirement
    unsafe { _mm256_loadu_si256(bytes.as_ptr().cast()) }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
fn store_bytes_avx2(bytes: &mut [u8; 32], value: __m256i) {
    // SAFETY: bytes is valid for a 32 byte write, and storeu has no alignment requirement
    unsafe { _mm256_storeu_si256(bytes.as_mut_ptr().cast(), value) }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
fn load_u64s_avx2(values: &[u64; 4]) -> __m256i {
    // SAFETY: values is valid for a 32 byte read, and loadu has no alignment requirement
    unsafe { _mm256_loadu_si256(values.as_ptr().cast()) }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
fn store_u64s_avx2(values: &mut [u64; 4], value: __m256i) {
    // SAFETY: values is valid for a 32 byte write, and storeu has no alignment requirement
    unsafe { _mm256_storeu_si256(values.as_mut_ptr().cast(), value) }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx512f")]
fn load_bytes_avx512(bytes: &[u8; 64]) -> __m512i {
    // SAFETY: bytes is valid for a 64 byte read, and loadu has no alignment requirement
    unsafe { _mm512_loadu_si512(bytes.as_ptr().cast()) }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx512f")]
fn store_bytes_avx512(bytes: &mut [u8; 64], value: __m512i) {
    // SAFETY: bytes is valid for a 64 byte write, and storeu has no alignment requirement
    unsafe { _mm512_storeu_si512(bytes.as_mut_ptr().cast(), value) }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx512f")]
fn load_u64s_avx512(values: &[u64; 8]) -> __m512i {
    // SAFETY: values is valid for a 64 byte read, and loadu has no alignment requirement
    unsafe { _mm512_loadu_si512(values.as_ptr().cast()) }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx512f")]
fn store_u64s_avx512(values: &mut [u64; 8], value: __m512i) {
    // SAFETY: values is valid for a 64 byte write, and storeu has no alignment requirement
    unsafe { _mm512_storeu_si512(values.as_mut_ptr().cast(), value) }
}

#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
#[target_feature(enable = "neon")]
fn load_bytes_neon(bytes: &[u8; 16]) -> uint8x16_t {
    // SAFETY: bytes is valid for a 16 byte read, and vld1q_u8 needs only u8 alignment
    unsafe { vld1q_u8(bytes.as_ptr()) }
}

#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
#[target_feature(enable = "neon")]
fn load_u64s_neon(values: &[u64; 2]) -> uint64x2_t {
    // SAFETY: values is valid for a 16 byte read, and is u64 aligned as vld1q_u64 requires
    unsafe { vld1q_u64(values.as_ptr()) }
}

#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
#[target_feature(enable = "neon")]
fn store_u64s_neon(values: &mut [u64; 2], value: uint64x2_t) {
    // SAFETY: values is valid for a 16 byte write, and is u64 aligned as vst1q_u64 requires
    unsafe { vst1q_u64(values.as_mut_ptr(), value) }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
fn scramble_accumulators_avx2(
    accumulators: &mut [u64; INIT_ACCUMULATORS.len()],
    secret: &[u8; STRIPE_LENGTH],
) {
    #[allow(clippy::cast_possible_truncation)]
    let simd_prime = _mm256_set1_epi32(PRIME32[0] as i32);

    let (accumulators, _) = accumulators.as_chunks_mut::<4>();
    let (secret, _) = secret.as_chunks::<32>();
    for (accumulator, secret) in accumulators.iter_mut().zip(secret) {
        let a = load_u64s_avx2(accumulator);
        let shifted = _mm256_srli_epi64::<47>(a);
        let b = _mm256_xor_si256(a, shifted);

        let s = load_bytes_avx2(secret);
        let c = _mm256_xor_si256(b, s);
        let c_high = _mm256_shuffle_epi32::<49>(c);

        let low = _mm256_mul_epu32(c, simd_prime);
        let high = _mm256_mul_epu32(c_high, simd_prime);
        let high = _mm256_slli_epi64::<32>(high);
        let result = _mm256_add_epi64(low, high);
        store_u64s_avx2(accumulator, result);
    }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx512f")]
fn scramble_accumulators_avx512(
    accumulators: &mut [u64; INIT_ACCUMULATORS.len()],
    secret: &[u8; STRIPE_LENGTH],
) {
    #[allow(clippy::cast_possible_truncation)]
    let simd_prime = _mm512_set1_epi32(PRIME32[0] as i32);

    let a = load_u64s_avx512(accumulators);
    let shifted = _mm512_srli_epi64::<47>(a);
    let b = _mm512_xor_si512(a, shifted);

    let s = load_bytes_avx512(secret);
    let c = _mm512_xor_si512(b, s);
    let c_high = _mm512_shuffle_epi32::<49>(c);

    let low = _mm512_mul_epu32(c, simd_prime);
    let high = _mm512_mul_epu32(c_high, simd_prime);
    let high = _mm512_slli_epi64::<32>(high);
    let result = _mm512_add_epi64(low, high);
    store_u64s_avx512(accumulators, result);
}

#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
#[target_feature(enable = "neon")]
fn scramble_accumulators_neon(
    accumulators: &mut [u64; INIT_ACCUMULATORS.len()],
    secret: &[u8; STRIPE_LENGTH],
) {
    let prime = vdup_n_u32(PRIME32[0].try_into().unwrap());

    let (accumulators, _) = accumulators.as_chunks_mut::<2>();
    let (secret, _) = secret.as_chunks::<16>();
    for (accumulator, secret) in accumulators.iter_mut().zip(secret) {
        // xorshift
        let accum = load_u64s_neon(accumulator);
        let shifted = vshrq_n_u64(accum, 47);
        let accum = veorq_u64(accum, shifted);

        // xor with secret
        let s = load_bytes_neon(secret);
        let accum = veorq_u64(accum, vreinterpretq_u64_u8(s));

        // mul with prime. Sadly there's no vmulq_u64
        let accum_low = vmovn_u64(accum);
        let accum_high = vshrn_n_u64(accum, 32);
        let prod_high = vshlq_n_u64(vmull_u32(accum_high, prime), 32);
        let accum = vmlal_u32(prod_high, accum_low, prime);
        store_u64s_neon(accumulator, accum);
    }
}

fn scramble_accumulators_generic(
    accumulators: &mut [u64; INIT_ACCUMULATORS.len()],
    secret: &[u8; STRIPE_LENGTH],
) {
    for (i, x) in accumulators.iter_mut().enumerate() {
        let s = get_u64(secret, i);
        *x = xorshift(*x, 47);
        *x ^= s;
        *x = x.wrapping_mul(PRIME32[0]);
    }
}

fn xorshift(x: u64, shift: u64) -> u64 {
    x ^ (x >> shift)
}

fn rrmxmx(mut x: u64, y: u64) -> u64 {
    x ^= x.rotate_left(49) ^ x.rotate_left(24);
    x = x.wrapping_mul(0x9FB21C651E98DF25);
    x ^= (x >> 35).wrapping_add(y);
    x = x.wrapping_mul(0x9FB21C651E98DF25);
    xorshift(x, 28)
}

fn mul128_and_xor(x: u64, y: u64) -> u64 {
    let z = u128::from(x) * u128::from(y);
    #[allow(clippy::cast_possible_truncation)]
    (z as u64 ^ (z >> 64) as u64)
}

fn mix16(data: &[u8], secret: &[u8], seed: u64) -> u64 {
    let x1 = get_u64(data, 0);
    let x2 = get_u64(data, 1);
    let s1 = get_u64(secret, 0).wrapping_add(seed);
    let s2 = get_u64(secret, 1).wrapping_sub(seed);

    mul128_and_xor(x1 ^ s1, x2 ^ s2)
}

fn mix32(state: (u64, u64), data1: &[u8], data2: &[u8], secret: &[u8], seed: u64) -> (u64, u64) {
    let (mut r_low, mut r_high) = state;

    r_low = r_low.wrapping_add(mix16(data1, secret, seed));
    r_low ^= get_u64(data2, 0).wrapping_add(get_u64(data2, 1));
    r_high = r_high.wrapping_add(mix16(data2, &secret[16..], seed));
    r_high ^= get_u64(data1, 0).wrapping_add(get_u64(data1, 1));

    (r_low, r_high)
}

fn gen_secret_generic(seed: u64) -> [u8; DEFAULT_SECRET.len()] {
    let mut secret = [0u8; DEFAULT_SECRET.len()];
    let iterations = DEFAULT_SECRET.len() / 16;
    for i in 0..iterations {
        let x = get_u64(&DEFAULT_SECRET, 2 * i).wrapping_add(seed);
        secret[16 * i..16 * i + 8].copy_from_slice(&x.to_le_bytes());
        let x = get_u64(&DEFAULT_SECRET, 2 * i + 1).wrapping_sub(seed);
        secret[16 * i + 8..16 * (i + 1)].copy_from_slice(&x.to_le_bytes());
    }
    secret
}

#[allow(clippy::cast_possible_truncation)]
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
fn gen_secret_avx2(seed: u64) -> [u8; DEFAULT_SECRET.len()] {
    #[allow(clippy::cast_possible_wrap)]
    let xxh_i64 = 0u64.wrapping_sub(seed) as i64;
    #[allow(clippy::cast_possible_wrap)]
    let seed = seed as i64;

    let simd_seed = _mm256_set_epi64x(xxh_i64, seed, xxh_i64, seed);

    let mut output = [0u8; DEFAULT_SECRET.len()];
    let (default_secret, _) = DEFAULT_SECRET.as_chunks::<32>();
    let (output_chunks, _) = output.as_chunks_mut::<32>();
    for (input, output) in default_secret.iter().zip(output_chunks) {
        let s = load_bytes_avx2(input);
        let x = _mm256_add_epi64(s, simd_seed);
        store_bytes_avx2(output, x);
    }

    output
}

#[allow(clippy::cast_possible_truncation)]
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx512f")]
fn gen_secret_avx512(seed: u64) -> [u8; DEFAULT_SECRET.len()] {
    #[allow(clippy::cast_possible_wrap)]
    let xxh_i64 = 0u64.wrapping_sub(seed) as i64;
    #[allow(clippy::cast_possible_wrap)]
    let seed = seed as i64;

    let simd_seed = _mm512_set_epi64(xxh_i64, seed, xxh_i64, seed, xxh_i64, seed, xxh_i64, seed);

    let mut output = [0u8; DEFAULT_SECRET.len()];
    let (default_secret, _) = DEFAULT_SECRET.as_chunks::<64>();
    let (output_chunks, _) = output.as_chunks_mut::<64>();
    for (input, output) in default_secret.iter().zip(output_chunks) {
        let s = load_bytes_avx512(input);
        let x = _mm512_add_epi64(s, simd_seed);
        store_bytes_avx512(output, x);
    }

    output
}

#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
#[target_feature(enable = "neon")]
fn accumulate_stripe_neon(
    accumulators: &mut [u64; INIT_ACCUMULATORS.len()],
    data: &[u8; STRIPE_LENGTH],
    secret: &[u8; STRIPE_LENGTH],
) {
    let (accumulators, _) = accumulators.as_chunks_mut::<2>();
    let (data, _) = data.as_chunks::<16>();
    let (secret, _) = secret.as_chunks::<16>();
    for ((accumulator, data), secret) in accumulators.iter_mut().zip(data).zip(secret) {
        let x = load_bytes_neon(data);
        let s = load_bytes_neon(secret);
        let x64 = vreinterpretq_u64_u8(x);
        let y = vextq_u64(x64, x64, 1);

        let result = load_u64s_neon(accumulator);
        let result = vaddq_u64(result, y);

        let z = vreinterpretq_u64_u8(veorq_u8(x, s));
        let z_low = vmovn_u64(z);
        let z_high = vshrn_n_u64(z, 32);

        let result = vmlal_u32(result, z_low, z_high);
        store_u64s_neon(accumulator, result);
    }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
fn accumulate_stripe_avx2(
    accumulators: &mut [u64; INIT_ACCUMULATORS.len()],
    data: &[u8; STRIPE_LENGTH],
    secret: &[u8; STRIPE_LENGTH],
) {
    let (accumulators, _) = accumulators.as_chunks_mut::<4>();
    let (data, _) = data.as_chunks::<32>();
    let (secret, _) = secret.as_chunks::<32>();
    for ((accumulator, data), secret) in accumulators.iter_mut().zip(data).zip(secret) {
        let x = load_bytes_avx2(data);
        let s = load_bytes_avx2(secret);

        let z = _mm256_xor_si256(x, s);
        let z_low = _mm256_shuffle_epi32::<49>(z);

        let product = _mm256_mul_epu32(z, z_low);
        let shuffled = _mm256_shuffle_epi32::<78>(x);

        let result = load_u64s_avx2(accumulator);
        let result = _mm256_add_epi64(result, shuffled);
        let result = _mm256_add_epi64(result, product);
        store_u64s_avx2(accumulator, result);
    }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx512f")]
fn accumulate_stripe_avx512(
    accumulators: &mut [u64; INIT_ACCUMULATORS.len()],
    data: &[u8; STRIPE_LENGTH],
    secret: &[u8; STRIPE_LENGTH],
) {
    let x = load_bytes_avx512(data);
    let s = load_bytes_avx512(secret);

    let z = _mm512_xor_si512(x, s);
    let z_low = _mm512_shuffle_epi32::<49>(z);

    let product = _mm512_mul_epu32(z, z_low);
    let shuffled = _mm512_shuffle_epi32::<78>(x);

    let result = load_u64s_avx512(accumulators);
    let result = _mm512_add_epi64(result, shuffled);
    let result = _mm512_add_epi64(result, product);
    store_u64s_avx512(accumulators, result);
}

fn accumulate_stripe_generic(
    accumulators: &mut [u64; INIT_ACCUMULATORS.len()],
    data: &[u8; STRIPE_LENGTH],
    secret: &[u8; STRIPE_LENGTH],
) {
    for i in 0..accumulators.len() {
        let x = get_u64(data, i);
        let y = x ^ get_u64(secret, i);
        accumulators[i ^ 1] = accumulators[i ^ 1].wrapping_add(x);
        let z = (y & 0xFFFF_FFFF) * (y >> 32);
        accumulators[i] = accumulators[i].wrapping_add(z);
    }
}

fn hash64_0(secret: &[u8], seed: u64) -> u64 {
    let mut result = seed;
    result ^= get_u64(secret, 7);
    result ^= get_u64(secret, 8);
    xxh64_avalanche(result)
}

fn hash64_1to3(data: &[u8], secret: &[u8], seed: u64) -> u64 {
    let x1 = u32::from(data[0]);
    let x2 = u32::from(data[data.len() >> 1]);
    let x3 = u32::from(*data.last().unwrap());
    #[allow(clippy::cast_possible_truncation)]
    let x4 = data.len() as u32;

    let combined = u64::from((x1 << 16) | (x2 << 24) | x3 | (x4 << 8));
    let mut result = u64::from(get_u32(secret, 0) ^ get_u32(secret, 1));
    result = result.wrapping_add(seed);
    result ^= combined;
    xxh64_avalanche(result)
}

fn hash64_4to8(data: &[u8], secret: &[u8], mut seed: u64) -> u64 {
    #[allow(clippy::cast_possible_truncation)]
    let truncate_seed = seed as u32;
    seed ^= u64::from(truncate_seed.swap_bytes()) << 32;
    let x1 = u64::from(get_u32(data, 0));
    let x2 = u64::from(get_u32(&data[data.len() - 4..], 0));
    let x = x2 | (x1 << 32);
    let s = (get_u64(secret, 1) ^ get_u64(secret, 2)).wrapping_sub(seed);
    rrmxmx(x ^ s, data.len() as u64)
}

fn hash64_9to16(data: &[u8], secret: &[u8], seed: u64) -> u64 {
    let s1 = (get_u64(secret, 3) ^ get_u64(secret, 4)).wrapping_add(seed);
    let s2 = (get_u64(secret, 5) ^ get_u64(secret, 6)).wrapping_sub(seed);
    let x1 = get_u64(data, 0) ^ s1;
    let x2 = get_u64(&data[data.len() - 8..], 0) ^ s2;
    let mut result = data.len() as u64;
    result = result.wrapping_add(x1.swap_bytes());
    result = result.wrapping_add(x2);
    result = result.wrapping_add(mul128_and_xor(x1, x2));
    xxh3_avalanche(result)
}

fn hash64_0to16(data: &[u8], secret: &[u8], seed: u64) -> u64 {
    if data.is_empty() {
        hash64_0(secret, seed)
    } else if data.len() < 4 {
        hash64_1to3(data, secret, seed)
    } else if data.len() <= 8 {
        hash64_4to8(data, secret, seed)
    } else {
        hash64_9to16(data, secret, seed)
    }
}

fn hash64_17to128(data: &[u8], secret: &[u8], seed: u64) -> u64 {
    let mut result = PRIME64[0].wrapping_mul(data.len() as u64);
    let iterations = (data.len() - 1) / 32;
    for i in (0..=iterations).rev() {
        result = result.wrapping_add(mix16(&data[16 * i..], &secret[32 * i..], seed));
        result = result.wrapping_add(mix16(
            &data[data.len() - 16 * (i + 1)..],
            &secret[32 * i + 16..],
            seed,
        ));
    }
    xxh3_avalanche(result)
}

fn hash64_129to240(data: &[u8], secret: &[u8], seed: u64) -> u64 {
    let mut result = PRIME64[0].wrapping_mul(data.len() as u64);
    for i in 0..8 {
        result = result.wrapping_add(mix16(&data[16 * i..], &secret[16 * i..], seed));
    }
    result = xxh3_avalanche(result);
    let iterations = data.len() / 16;
    for i in 8..iterations {
        result = result.wrapping_add(mix16(&data[16 * i..], &secret[16 * (i - 8) + 3..], seed));
    }
    result = result.wrapping_add(mix16(
        &data[data.len() - 16..],
        &secret[MIN_SECRET_SIZE - 17..],
        seed,
    ));

    xxh3_avalanche(result)
}

fn hash64_0to240(data: &[u8], secret: &[u8], seed: u64) -> u64 {
    if data.len() <= 16 {
        hash64_0to16(data, secret, seed)
    } else if data.len() <= 128 {
        hash64_17to128(data, secret, seed)
    } else {
        hash64_129to240(data, secret, seed)
    }
}

fn hash64_large_generic(data: &[u8], seed: u64) -> u64 {
    hash64_large_impl!(
        data,
        seed,
        gen_secret_generic,
        scramble_accumulators_generic,
        accumulate_stripe_generic
    )
}

#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
#[target_feature(enable = "neon")]
fn hash64_large_neon(data: &[u8], seed: u64) -> u64 {
    hash64_large_impl!(
        data,
        seed,
        gen_secret_generic,
        scramble_accumulators_neon,
        accumulate_stripe_neon
    )
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
fn hash64_large_avx2(data: &[u8], seed: u64) -> u64 {
    hash64_large_impl!(
        data,
        seed,
        gen_secret_avx2,
        scramble_accumulators_avx2,
        accumulate_stripe_avx2
    )
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx512f")]
fn hash64_large_avx512(data: &[u8], seed: u64) -> u64 {
    hash64_large_impl!(
        data,
        seed,
        gen_secret_avx512,
        scramble_accumulators_avx512,
        accumulate_stripe_avx512
    )
}

fn hash128_0(secret: &[u8], seed: u64) -> u128 {
    let high = u128::from(hash64_0(&secret[3 * 8..], seed)) << 64;
    let low = u128::from(hash64_0(&secret[8..], seed));
    high | low
}

fn hash128_1to3(data: &[u8], secret: &[u8], seed: u64) -> u128 {
    let x1 = u32::from(data[0]);
    let x2 = u32::from(data[data.len() >> 1]);
    let x3 = u32::from(*data.last().unwrap());
    #[allow(clippy::cast_possible_truncation)]
    let x4 = data.len() as u32;

    let combined_low = (x1 << 16) | (x2 << 24) | x3 | (x4 << 8);
    let combined_high: u64 = combined_low.swap_bytes().rotate_left(13).into();
    let s_low = u64::from(get_u32(secret, 0) ^ get_u32(secret, 1)).wrapping_add(seed);
    let s_high = u64::from(get_u32(secret, 2) ^ get_u32(secret, 3)).wrapping_sub(seed);
    let high = u128::from(xxh64_avalanche(combined_high ^ s_high)) << 64;
    let low = u128::from(xxh64_avalanche(u64::from(combined_low) ^ s_low));
    high | low
}

fn hash128_4to8(data: &[u8], secret: &[u8], mut seed: u64) -> u128 {
    #[allow(clippy::cast_possible_truncation)]
    let truncate_seed = seed as u32;
    seed ^= u64::from(truncate_seed.swap_bytes()) << 32;
    let x_low = u64::from(get_u32(data, 0));
    let x_high = u64::from(u32::from_le_bytes(
        data[data.len() - 4..].try_into().unwrap(),
    ));
    let x = x_low | (x_high << 32);
    let s = (get_u64(secret, 2) ^ get_u64(secret, 3)).wrapping_add(seed);

    let mut y = u128::from(x ^ s);
    y = y.wrapping_mul(u128::from(
        PRIME64[0].wrapping_add((data.len() << 2) as u64),
    ));

    #[allow(clippy::cast_possible_truncation)]
    let mut r_low = y as u64;
    let mut r_high: u64 = (y >> 64).try_into().unwrap();
    r_high = r_high.wrapping_add(r_low << 1);
    r_low ^= r_high >> 3;
    r_low = xorshift(r_low, 35);
    r_low = r_low.wrapping_mul(0x9FB21C651E98DF25);
    r_low = xorshift(r_low, 28);
    r_high = xxh3_avalanche(r_high);

    (u128::from(r_high) << 64) | u128::from(r_low)
}

fn hash128_9to16(data: &[u8], secret: &[u8], seed: u64) -> u128 {
    let s_low = (get_u64(secret, 4) ^ get_u64(secret, 5)).wrapping_sub(seed);
    let s_high = (get_u64(secret, 6) ^ get_u64(secret, 7)).wrapping_add(seed);
    let x_low = get_u64(data, 0);
    let x_high = u64::from_le_bytes(data[data.len() - 8..].try_into().unwrap());
    let mixed = x_low ^ x_high ^ s_low;
    let x_high = x_high ^ s_high;

    let result = u128::from(mixed).wrapping_mul(u128::from(PRIME64[0]));
    #[allow(clippy::cast_possible_truncation)]
    let mut r_low = result as u64;
    let mut r_high = (result >> 64) as u64;
    r_low = r_low.wrapping_add((data.len() as u64 - 1) << 54);
    r_high = r_high.wrapping_add(x_high);
    r_high = r_high.wrapping_add((x_high & 0xFFFF_FFFF).wrapping_mul(PRIME32[1] - 1));
    r_low ^= r_high.swap_bytes();

    let result2 = u128::from(r_low).wrapping_mul(u128::from(PRIME64[1]));
    #[allow(clippy::cast_possible_truncation)]
    let mut r2_low = result2 as u64;
    let mut r2_high = (result2 >> 64) as u64;
    r2_high = r2_high.wrapping_add(r_high.wrapping_mul(PRIME64[1]));
    r2_low = xxh3_avalanche(r2_low);
    r2_high = xxh3_avalanche(r2_high);

    (u128::from(r2_high) << 64) | u128::from(r2_low)
}

fn hash128_0to16(data: &[u8], secret: &[u8], seed: u64) -> u128 {
    if data.is_empty() {
        hash128_0(secret, seed)
    } else if data.len() < 4 {
        hash128_1to3(data, secret, seed)
    } else if data.len() <= 8 {
        hash128_4to8(data, secret, seed)
    } else {
        hash128_9to16(data, secret, seed)
    }
}

fn hash128_17to128(data: &[u8], secret: &[u8], seed: u64) -> u128 {
    let len = data.len();
    let mut state = (PRIME64[0].wrapping_mul(len as u64), 0);
    if len > 32 {
        if len > 64 {
            if len > 96 {
                state = mix32(state, &data[48..], &data[len - 64..], &secret[96..], seed);
            }
            state = mix32(state, &data[32..], &data[len - 48..], &secret[64..], seed);
        }
        state = mix32(state, &data[16..], &data[len - 32..], &secret[32..], seed);
    }
    state = mix32(state, data, &data[len - 16..], secret, seed);

    let mut r_low = state.0.wrapping_add(state.1);
    let mut r_high = state.0.wrapping_mul(PRIME64[0]);
    r_high = r_high.wrapping_add(state.1.wrapping_mul(PRIME64[3]));
    r_high = r_high.wrapping_add((len as u64).wrapping_sub(seed).wrapping_mul(PRIME64[1]));
    r_low = xxh3_avalanche(r_low);
    r_high = 0u64.wrapping_sub(xxh3_avalanche(r_high));

    (u128::from(r_high) << 64) | u128::from(r_low)
}

fn hash128_129to240(data: &[u8], secret: &[u8], seed: u64) -> u128 {
    let len = data.len();
    let iterations = len / 32;
    let mut state = (PRIME64[0].wrapping_mul(len as u64), 0);

    for i in 0..4 {
        state = mix32(
            state,
            &data[32 * i..],
            &data[32 * i + 16..],
            &secret[32 * i..],
            seed,
        );
    }
    state.0 = xxh3_avalanche(state.0);
    state.1 = xxh3_avalanche(state.1);

    for i in 4..iterations {
        state = mix32(
            state,
            &data[32 * i..],
            &data[32 * i + 16..],
            &secret[3 + 32 * (i - 4)..],
            seed,
        );
    }
    state = mix32(
        state,
        &data[len - 16..],
        &data[len - 32..],
        &secret[MIN_SECRET_SIZE - 33..],
        0u64.wrapping_sub(seed),
    );

    let mut r_low = state.0.wrapping_add(state.1);
    let mut r_high = state.0.wrapping_mul(PRIME64[0]);
    r_high = r_high.wrapping_add(state.1.wrapping_mul(PRIME64[3]));
    r_high = r_high.wrapping_add((len as u64).wrapping_sub(seed).wrapping_mul(PRIME64[1]));
    r_low = xxh3_avalanche(r_low);
    r_high = 0u64.wrapping_sub(xxh3_avalanche(r_high));

    (u128::from(r_high) << 64) | u128::from(r_low)
}

fn hash128_0to240(data: &[u8], secret: &[u8], seed: u64) -> u128 {
    if data.len() <= 16 {
        hash128_0to16(data, secret, seed)
    } else if data.len() <= 128 {
        hash128_17to128(data, secret, seed)
    } else {
        hash128_129to240(data, secret, seed)
    }
}

fn hash128_large_generic(data: &[u8], seed: u64) -> u128 {
    hash128_large_impl!(
        data,
        seed,
        gen_secret_generic,
        scramble_accumulators_generic,
        accumulate_stripe_generic
    )
}

#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
#[target_feature(enable = "neon")]
fn hash128_large_neon(data: &[u8], seed: u64) -> u128 {
    hash128_large_impl!(
        data,
        seed,
        gen_secret_generic,
        scramble_accumulators_neon,
        accumulate_stripe_neon
    )
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
fn hash128_large_avx2(data: &[u8], seed: u64) -> u128 {
    hash128_large_impl!(
        data,
        seed,
        gen_secret_avx2,
        scramble_accumulators_avx2,
        accumulate_stripe_avx2
    )
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx512f")]
fn hash128_large_avx512(data: &[u8], seed: u64) -> u128 {
    hash128_large_impl!(
        data,
        seed,
        gen_secret_avx512,
        scramble_accumulators_avx512,
        accumulate_stripe_avx512
    )
}
