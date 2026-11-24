/* libc routines that wasi-libc implements a byte at a time, replaced with
 * 16-byte WebAssembly SIMD versions. SQLite's instr(), LIKE/GLOB and
 * b-tree key comparisons spend their time in memcmp(); with the scalar
 * one, instr() on long strings ran about 16 times slower than natively.
 * Built with -msimd128 -fno-builtin (so the loops are not turned back into
 * calls to themselves); the linker takes these before libc's. */
#include <stddef.h>
#include <stdint.h>
#include <wasm_simd128.h>

int memcmp(const void *a, const void *b, size_t n) {
    const unsigned char *p = a, *q = b;
    /* 64 bytes per iteration: wasmtime's epoch check sits on every loop
     * back-edge, so fewer iterations means less of its cost. */
    while (n >= 64) {
        v128_t e = wasm_v128_and(
            wasm_v128_and(wasm_i8x16_eq(wasm_v128_load(p), wasm_v128_load(q)),
                          wasm_i8x16_eq(wasm_v128_load(p + 16), wasm_v128_load(q + 16))),
            wasm_v128_and(wasm_i8x16_eq(wasm_v128_load(p + 32), wasm_v128_load(q + 32)),
                          wasm_i8x16_eq(wasm_v128_load(p + 48), wasm_v128_load(q + 48))));
        if (!wasm_i8x16_all_true(e))
            break;
        p += 64;
        q += 64;
        n -= 64;
    }
    while (n >= 16) {
        v128_t x = wasm_v128_load(p), y = wasm_v128_load(q);
        if (!wasm_i8x16_all_true(wasm_i8x16_eq(x, y)))
            break;
        p += 16;
        q += 16;
        n -= 16;
    }
    for (; n > 0; n--, p++, q++)
        if (*p != *q)
            return *p - *q;
    return 0;
}
