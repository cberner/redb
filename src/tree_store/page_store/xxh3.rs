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

// Copied from xxh3 crate, commit hash a2bfd3a

use std::mem::size_of;

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

#[allow(clippy::needless_return)]
pub fn hash64_with_seed(data: &[u8], seed: u64) -> u64 {
    if data.len() <= 240 {
        hash64_0to240(data, &DEFAULT_SECRET, seed)
    } else {
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            if is_x86_feature_detected!("avx2") {
                unsafe {
                    return hash64_large_avx2(data, seed);
                }
            }
        }
        #[cfg(target_arch = "aarch64")]
        {
            unsafe {
                return hash64_large_neon(data, seed);
            }
        }
        #[cfg(not(target_arch = "aarch64"))]
        hash64_large_generic(
            data,
            seed,
            gen_secret_generic,
            scramble_accumulators_generic,
            accumulate_stripe_generic,
        )
    }
}

#[allow(clippy::needless_return)]
pub fn hash128_with_seed(data: &[u8], seed: u64) -> u128 {
    if data.len() <= 240 {
        hash128_0to240(data, &DEFAULT_SECRET, seed)
    } else {
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        if is_x86_feature_detected!("avx2") {
            unsafe {
                return hash128_large_avx2(data, seed);
            }
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            return hash128_large_neon(data, seed);
        }
        #[cfg(not(target_arch = "aarch64"))]
        hash128_large_generic(
            data,
            seed,
            gen_secret_generic,
            scramble_accumulators_generic,
            accumulate_stripe_generic,
        )
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

#[inline(always)]
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

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn scramble_accumulators_avx2(
    accumulators: &mut [u64; INIT_ACCUMULATORS.len()],
    secret: &[u8],
) {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::*;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    #[allow(clippy::cast_possible_truncation)]
    let simd_prime = _mm256_set1_epi32(PRIME32[0] as i32);
    let secret_ptr = secret.as_ptr();
    let accumulators_ptr = accumulators.as_mut_ptr();

    for i in 0..(STRIPE_LENGTH / 32) {
        let a = _mm256_loadu_si256((accumulators_ptr as *const __m256i).add(i));
        let shifted = _mm256_srli_epi64::<47>(a);
        let b = _mm256_xor_si256(a, shifted);

        let s = _mm256_loadu_si256((secret_ptr as *const __m256i).add(i));
        let c = _mm256_xor_si256(b, s);
        let c_high = _mm256_shuffle_epi32::<49>(c);

        let low = _mm256_mul_epu32(c, simd_prime);
        let high = _mm256_mul_epu32(c_high, simd_prime);
        let high = _mm256_slli_epi64::<32>(high);
        let result = _mm256_add_epi64(low, high);
        _mm256_storeu_si256((accumulators_ptr as *mut __m256i).add(i), result);
    }
}

#[cfg(target_arch = "aarch64")]
unsafe fn scramble_accumulators_neon(
    accumulators: &mut [u64; INIT_ACCUMULATORS.len()],
    secret: &[u8],
) {
    #[cfg(target_arch = "aarch64")]
    use std::arch::aarch64::*;
    #[cfg(target_arch = "arm")]
    use std::arch::arm::*;

    let prime = vdup_n_u32(PRIME32[0].try_into().unwrap());

    let accum_ptr = accumulators.as_mut_ptr();
    let secret_ptr = secret.as_ptr();
    assert!(secret.len() >= STRIPE_LENGTH);
    for i in 0..(STRIPE_LENGTH / 16) {
        // xorshift
        let accum = vld1q_u64(accum_ptr.add(i * 2));
        let shifted = vshrq_n_u64(accum, 47);
        let accum = veorq_u64(accum, shifted);

        // xor with secret
        let s = vld1q_u8(secret_ptr.add(i * 16));
        let accum = veorq_u64(accum, vreinterpretq_u64_u8(s));

        // mul with prime. Sadly there's no vmulq_u64
        let accum_low = vmovn_u64(accum);
        let accum_high = vshrn_n_u64(accum, 32);
        let prod_high = vshlq_n_u64(vmull_u32(accum_high, prime), 32);
        let accum = vmlal_u32(prod_high, accum_low, prime);
        vst1q_u64(accum_ptr.add(i * 2), accum);
    }
}

#[cfg(not(target_arch = "aarch64"))]
fn scramble_accumulators_generic(accumulators: &mut [u64; INIT_ACCUMULATORS.len()], secret: &[u8]) {
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
unsafe fn gen_secret_avx2(seed: u64) -> [u8; DEFAULT_SECRET.len()] {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::*;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    #[allow(clippy::cast_possible_wrap)]
    let xxh_i64 = 0u64.wrapping_sub(seed) as i64;
    #[allow(clippy::cast_possible_wrap)]
    let seed = seed as i64;

    let simd_seed = _mm256_set_epi64x(xxh_i64, seed, xxh_i64, seed);

    let mut output = [0u8; DEFAULT_SECRET.len()];
    let output_ptr = output.as_mut_ptr();
    let secret_ptr = DEFAULT_SECRET.as_ptr();
    for i in 0..6 {
        let s = _mm256_loadu_si256((secret_ptr as *const __m256i).add(i));
        let x = _mm256_add_epi64(s, simd_seed);
        _mm256_storeu_si256((output_ptr as *mut __m256i).add(i), x);
    }

    output
}

#[cfg(target_arch = "aarch64")]
unsafe fn accumulate_stripe_neon(accumulators: &mut [u64; 8], data: &[u8], secret: &[u8]) {
    #[cfg(target_arch = "aarch64")]
    use std::arch::aarch64::*;
    #[cfg(target_arch = "arm")]
    use std::arch::arm::*;

    let accum_ptr = accumulators.as_mut_ptr();
    let data_ptr = data.as_ptr();
    let secret_ptr = secret.as_ptr();
    assert!(data.len() >= STRIPE_LENGTH);
    assert!(secret.len() >= STRIPE_LENGTH);
    for i in 0..(STRIPE_LENGTH / 16) {
        let x = vld1q_u8(data_ptr.add(i * 16));
        let s = vld1q_u8(secret_ptr.add(i * 16));
        let x64 = vreinterpretq_u64_u8(x);
        let y = vextq_u64(x64, x64, 1);

        let result = vld1q_u64(accum_ptr.add(i * 2));
        let result = vaddq_u64(result, y);

        let z = vreinterpretq_u64_u8(veorq_u8(x, s));
        let z_low = vmovn_u64(z);
        let z_high = vshrn_n_u64(z, 32);

        let result = vmlal_u32(result, z_low, z_high);
        vst1q_u64(accum_ptr.add(i * 2), result);
    }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn accumulate_stripe_avx2(accumulators: &mut [u64; 8], data: &[u8], secret: &[u8]) {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::*;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    let data_ptr = data.as_ptr();
    let secret_ptr = secret.as_ptr();
    let accumulator_ptr = accumulators.as_mut_ptr();

    assert!(data.len() >= STRIPE_LENGTH);
    assert!(secret.len() >= STRIPE_LENGTH);
    for i in 0..(STRIPE_LENGTH / 32) {
        let x = _mm256_loadu_si256((data_ptr as *const __m256i).add(i));
        let s = _mm256_loadu_si256((secret_ptr as *const __m256i).add(i));

        let z = _mm256_xor_si256(x, s);
        let z_low = _mm256_shuffle_epi32::<49>(z);

        let product = _mm256_mul_epu32(z, z_low);
        let shuffled = _mm256_shuffle_epi32::<78>(x);

        let result = _mm256_loadu_si256((accumulator_ptr as *const __m256i).add(i));
        let result = _mm256_add_epi64(result, shuffled);
        let result = _mm256_add_epi64(result, product);
        _mm256_storeu_si256((accumulator_ptr as *mut __m256i).add(i), result);
    }
}

#[cfg(not(target_arch = "aarch64"))]
fn accumulate_stripe_generic(accumulators: &mut [u64; 8], data: &[u8], secret: &[u8]) {
    for i in 0..accumulators.len() {
        let x = get_u64(&data[i * 8..], 0);
        let y = x ^ get_u64(&secret[i * 8..], 0);
        accumulators[i ^ 1] = accumulators[i ^ 1].wrapping_add(x);
        let z = (y & 0xFFFF_FFFF) * (y >> 32);
        accumulators[i] = accumulators[i].wrapping_add(z)
    }
}

#[inline(always)]
fn accumulate_block(
    accumulators: &mut [u64; 8],
    data: &[u8],
    secret: &[u8],
    stripes: usize,
    accum_stripe: unsafe fn(&mut [u64; 8], &[u8], &[u8]),
) {
    for i in 0..stripes {
        unsafe {
            accum_stripe(
                accumulators,
                &data[i * STRIPE_LENGTH..],
                &secret[i * SECRET_CONSUME_RATE..],
            );
        }
    }
}

#[inline(always)]
fn hash_large_helper(
    data: &[u8],
    secret: &[u8],
    scramble: unsafe fn(&mut [u64; 8], &[u8]),
    accum_stripe: unsafe fn(&mut [u64; 8], &[u8], &[u8]),
) -> [u64; INIT_ACCUMULATORS.len()] {
    let mut accumulators = INIT_ACCUMULATORS;

    let stripes_per_block = (secret.len() - STRIPE_LENGTH) / SECRET_CONSUME_RATE;
    let block_len = STRIPE_LENGTH * stripes_per_block;
    let blocks = (data.len() - 1) / block_len;

    // accumulate all the blocks
    for i in 0..blocks {
        accumulate_block(
            &mut accumulators,
            &data[i * block_len..],
            secret,
            stripes_per_block,
            accum_stripe,
        );
        unsafe { scramble(&mut accumulators, &secret[secret.len() - STRIPE_LENGTH..]) };
    }

    // trailing partial block
    let stripes = ((data.len() - 1) - block_len * blocks) / STRIPE_LENGTH;
    accumulate_block(
        &mut accumulators,
        &data[blocks * block_len..],
        secret,
        stripes,
        accum_stripe,
    );

    // trailing stripe
    unsafe {
        accum_stripe(
            &mut accumulators,
            &data[data.len() - STRIPE_LENGTH..],
            &secret[secret.len() - STRIPE_LENGTH - 7..],
        );
    }

    accumulators
}

fn hash64_0(secret: &[u8], seed: u64) -> u64 {
    let mut result = seed;
    result ^= get_u64(secret, 7);
    result ^= get_u64(secret, 8);
    xxh64_avalanche(result)
}

fn hash64_1to3(data: &[u8], secret: &[u8], seed: u64) -> u64 {
    let x1 = data[0] as u32;
    let x2 = data[data.len() >> 1] as u32;
    let x3 = (*data.last().unwrap()) as u32;
    #[allow(clippy::cast_possible_truncation)]
    let x4 = data.len() as u32;

    let combined = ((x1 << 16) | (x2 << 24) | x3 | (x4 << 8)) as u64;
    let mut result = (get_u32(secret, 0) ^ get_u32(secret, 1)) as u64;
    result = result.wrapping_add(seed);
    result ^= combined;
    xxh64_avalanche(result)
}

fn hash64_4to8(data: &[u8], secret: &[u8], mut seed: u64) -> u64 {
    #[allow(clippy::cast_possible_truncation)]
    let truncate_seed = seed as u32;
    seed ^= u64::from(truncate_seed.swap_bytes()) << 32;
    let x1 = get_u32(data, 0) as u64;
    let x2 = get_u32(&data[data.len() - 4..], 0) as u64;
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

#[cfg(target_arch = "aarch64")]
unsafe fn hash64_large_neon(data: &[u8], seed: u64) -> u64 {
    hash64_large_generic(
        data,
        seed,
        gen_secret_generic,
        scramble_accumulators_neon,
        accumulate_stripe_neon,
    )
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn hash64_large_avx2(data: &[u8], seed: u64) -> u64 {
    hash64_large_generic(
        data,
        seed,
        gen_secret_avx2,
        scramble_accumulators_avx2,
        accumulate_stripe_avx2,
    )
}

#[inline(always)]
fn hash64_large_generic(
    data: &[u8],
    seed: u64,
    gen: unsafe fn(u64) -> [u8; DEFAULT_SECRET.len()],
    scramble: unsafe fn(&mut [u64; 8], &[u8]),
    accum_stripe: unsafe fn(&mut [u64; 8], &[u8], &[u8]),
) -> u64 {
    let secret = unsafe { gen(seed) };
    let accumulators = hash_large_helper(data, &secret, scramble, accum_stripe);

    merge_accumulators(
        accumulators,
        &secret[11..],
        PRIME64[0].wrapping_mul(data.len() as u64),
    )
}

fn hash128_0(secret: &[u8], seed: u64) -> u128 {
    let high = (hash64_0(&secret[3 * 8..], seed) as u128) << 64;
    let low = hash64_0(&secret[8..], seed) as u128;
    high | low
}

fn hash128_1to3(data: &[u8], secret: &[u8], seed: u64) -> u128 {
    let x1 = data[0] as u32;
    let x2 = data[data.len() >> 1] as u32;
    let x3 = (*data.last().unwrap()) as u32;
    #[allow(clippy::cast_possible_truncation)]
    let x4 = data.len() as u32;

    let combined_low = (x1 << 16) | (x2 << 24) | x3 | (x4 << 8);
    let combined_high: u64 = combined_low.swap_bytes().rotate_left(13).into();
    let s_low = ((get_u32(secret, 0) ^ get_u32(secret, 1)) as u64).wrapping_add(seed);
    let s_high = ((get_u32(secret, 2) ^ get_u32(secret, 3)) as u64).wrapping_sub(seed);
    let high = (xxh64_avalanche(combined_high ^ s_high) as u128) << 64;
    let low = xxh64_avalanche(combined_low as u64 ^ s_low) as u128;
    high | low
}

fn hash128_4to8(data: &[u8], secret: &[u8], mut seed: u64) -> u128 {
    #[allow(clippy::cast_possible_truncation)]
    let truncate_seed = seed as u32;
    seed ^= u64::from(truncate_seed.swap_bytes()) << 32;
    let x_low = get_u32(data, 0) as u64;
    let x_high = u32::from_le_bytes(data[data.len() - 4..].try_into().unwrap()) as u64;
    let x = x_low | (x_high << 32);
    let s = (get_u64(secret, 2) ^ get_u64(secret, 3)).wrapping_add(seed);

    let mut y = (x ^ s) as u128;
    y = y.wrapping_mul(PRIME64[0].wrapping_add((data.len() << 2) as u64) as u128);

    #[allow(clippy::cast_possible_truncation)]
    let mut r_low = y as u64;
    let mut r_high: u64 = (y >> 64).try_into().unwrap();
    r_high = r_high.wrapping_add(r_low << 1);
    r_low ^= r_high >> 3;
    r_low = xorshift(r_low, 35);
    r_low = r_low.wrapping_mul(0x9FB21C651E98DF25);
    r_low = xorshift(r_low, 28);
    r_high = xxh3_avalanche(r_high);

    (r_high as u128) << 64 | r_low as u128
}

fn hash128_9to16(data: &[u8], secret: &[u8], seed: u64) -> u128 {
    let s_low = (get_u64(secret, 4) ^ get_u64(secret, 5)).wrapping_sub(seed);
    let s_high = (get_u64(secret, 6) ^ get_u64(secret, 7)).wrapping_add(seed);
    let x_low = get_u64(data, 0);
    let x_high = u64::from_le_bytes(data[data.len() - 8..].try_into().unwrap());
    let mixed = x_low ^ x_high ^ s_low;
    let x_high = x_high ^ s_high;

    let result = (mixed as u128).wrapping_mul(PRIME64[0] as u128);
    #[allow(clippy::cast_possible_truncation)]
    let mut r_low = result as u64;
    let mut r_high = (result >> 64) as u64;
    r_low = r_low.wrapping_add((data.len() as u64 - 1) << 54);
    r_high = r_high.wrapping_add(x_high);
    r_high = r_high.wrapping_add((x_high & 0xFFFF_FFFF).wrapping_mul(PRIME32[1] - 1));
    r_low ^= r_high.swap_bytes();

    let result2 = (r_low as u128).wrapping_mul(PRIME64[1] as u128);
    #[allow(clippy::cast_possible_truncation)]
    let mut r2_low = result2 as u64;
    let mut r2_high = (result2 >> 64) as u64;
    r2_high = r2_high.wrapping_add(r_high.wrapping_mul(PRIME64[1]));
    r2_low = xxh3_avalanche(r2_low);
    r2_high = xxh3_avalanche(r2_high);

    (r2_high as u128) << 64 | r2_low as u128
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

    (r_high as u128) << 64 | r_low as u128
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

    (r_high as u128) << 64 | r_low as u128
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

#[cfg(target_arch = "aarch64")]
unsafe fn hash128_large_neon(data: &[u8], seed: u64) -> u128 {
    hash128_large_generic(
        data,
        seed,
        gen_secret_generic,
        scramble_accumulators_neon,
        accumulate_stripe_neon,
    )
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn hash128_large_avx2(data: &[u8], seed: u64) -> u128 {
    hash128_large_generic(
        data,
        seed,
        gen_secret_avx2,
        scramble_accumulators_avx2,
        accumulate_stripe_avx2,
    )
}

#[inline(always)]
fn hash128_large_generic(
    data: &[u8],
    seed: u64,
    gen: unsafe fn(u64) -> [u8; DEFAULT_SECRET.len()],
    scramble: unsafe fn(&mut [u64; 8], &[u8]),
    accum_stripe: unsafe fn(&mut [u64; 8], &[u8], &[u8]),
) -> u128 {
    let secret = unsafe { gen(seed) };
    let accumulators = hash_large_helper(data, &secret, scramble, accum_stripe);

    let low = merge_accumulators(
        accumulators,
        &secret[11..],
        PRIME64[0].wrapping_mul(data.len() as u64),
    );
    let high = merge_accumulators(
        accumulators,
        &secret[secret.len() - 64 - 11..],
        !(PRIME64[1].wrapping_mul(data.len() as u64)),
    );

    (high as u128) << 64 | low as u128
}

#[cfg(test)]
mod test {
    use crate::tree_store::page_store::xxh3::hash64_with_seed;

    #[test]
    fn test_empty() {
        let actual = hash64_with_seed(&[], 0);
        assert_eq!(actual, 3244421341483603138);
    }
}
