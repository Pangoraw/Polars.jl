#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include "arrow.h"

typedef enum polars_value_type_t {
  PolarsValueTypeNull,
  PolarsValueTypeBoolean,
  PolarsValueTypeUInt8,
  PolarsValueTypeUInt16,
  PolarsValueTypeUInt32,
  PolarsValueTypeUInt64,
  PolarsValueTypeInt8,
  PolarsValueTypeInt16,
  PolarsValueTypeInt32,
  PolarsValueTypeInt64,
  PolarsValueTypeFloat32,
  PolarsValueTypeFloat64,
  PolarsValueTypeList,
  PolarsValueTypeUtf8,
  PolarsValueTypeStruct,
  PolarsValueTypeBinary,
  PolarsValueTypeUnknown,
} polars_value_type_t;

typedef struct polars_dataframe_t polars_dataframe_t;

typedef struct polars_error_t polars_error_t;

typedef struct polars_expr_t polars_expr_t;

typedef struct polars_lazy_frame_t polars_lazy_frame_t;

typedef struct polars_lazy_group_by_t polars_lazy_group_by_t;

typedef struct polars_series_t polars_series_t;

typedef struct polars_value_t polars_value_t;

/**
 * The callback provided for display functions, returns -1 on error.
 */
typedef intptr_t (*IOCallback)(const void *user, const uint8_t *data, uintptr_t len);

uintptr_t polars_version(const uint8_t **out);

uintptr_t polars_error_message(const struct polars_error_t *err, const uint8_t **data);

void polars_error_destroy(const struct polars_error_t *err);

void polars_dataframe_size(struct polars_dataframe_t *df, uintptr_t *rows, uintptr_t *cols);

/**
 * Creates a DataFrame from a series of ArrowArray and ArrowSchema compatible the arrow C-ABI.
 *
 * # Safety
 * The field array should be valid ArrowSchema according to the C Data Interface.
 * The array array should be valid ArrowArray according to the C Data Interface,
 * this means that the memory ownership is transferred in the created arrow::Array.
 * Therefore, the caller should *not* free the underlying memories for this arrow as this
 * will be done through the release field of the array.
 *
 * Returns null if something went wrong.
 */
struct polars_dataframe_t *polars_dataframe_new_from_carrow(const ArrowSchema *cfield,
                                                            ArrowArray carray);

/**
 * Returns a ArrowSchema describing the dataframe's schema according to Arrow C Data interface.
 */
ArrowSchema polars_dataframe_schema(struct polars_dataframe_t *df);

const struct polars_error_t *polars_dataframe_new_from_series(struct polars_series_t *const *series,
                                                              uintptr_t nseries,
                                                              struct polars_dataframe_t **out);

void polars_dataframe_destroy(struct polars_dataframe_t *df);

const struct polars_error_t *polars_dataframe_write_parquet(struct polars_dataframe_t *df,
                                                            const void *user,
                                                            IOCallback callback);

const struct polars_error_t *polars_dataframe_read_parquet(const uint8_t *path,
                                                           uintptr_t pathlen,
                                                           struct polars_dataframe_t **out);

void polars_dataframe_show(struct polars_dataframe_t *df, const void *user, IOCallback callback);

const struct polars_error_t *polars_dataframe_get(struct polars_dataframe_t *df,
                                                  const uint8_t *name,
                                                  uintptr_t len,
                                                  struct polars_series_t **out);

struct polars_lazy_frame_t *polars_dataframe_lazy(struct polars_dataframe_t *df);

void polars_lazy_frame_destroy(struct polars_lazy_frame_t *df);

struct polars_lazy_frame_t *polars_lazy_frame_clone(struct polars_lazy_frame_t *df);

void polars_lazy_frame_sort(struct polars_lazy_frame_t *df,
                            const struct polars_expr_t *const *exprs,
                            uintptr_t nexprs,
                            const bool *descending,
                            bool nulls_last,
                            bool maintain_order);

const struct polars_error_t *polars_lazy_frame_concat(struct polars_lazy_frame_t *const *lfs,
                                                      uintptr_t n,
                                                      struct polars_lazy_frame_t **out);

void polars_lazy_frame_with_columns(struct polars_lazy_frame_t *df,
                                    const struct polars_expr_t *const *exprs,
                                    uintptr_t nexprs);

void polars_lazy_frame_select(struct polars_lazy_frame_t *df,
                              const struct polars_expr_t *const *exprs,
                              uintptr_t nexprs);

void polars_lazy_frame_filter(struct polars_lazy_frame_t *df, const struct polars_expr_t *expr);

const struct polars_error_t *polars_lazy_frame_collect(struct polars_lazy_frame_t *df,
                                                       struct polars_dataframe_t **out);

struct polars_lazy_group_by_t *polars_lazy_frame_group_by(struct polars_lazy_frame_t *df,
                                                          const struct polars_expr_t *const *exprs,
                                                          uintptr_t nexprs);

struct polars_lazy_frame_t *polars_lazy_frame_join_inner(struct polars_lazy_frame_t *a,
                                                         struct polars_lazy_frame_t *b,
                                                         const struct polars_expr_t *const *exprs_a,
                                                         uintptr_t exprs_a_len,
                                                         const struct polars_expr_t *const *exprs_b,
                                                         uintptr_t exprs_b_len);

const struct polars_error_t *polars_lazy_frame_fetch(struct polars_lazy_frame_t *df,
                                                     uintptr_t n,
                                                     struct polars_dataframe_t **out);

void polars_lazy_group_by_destroy(const struct polars_lazy_group_by_t *gb);

struct polars_lazy_frame_t *polars_lazy_group_by_agg(struct polars_lazy_group_by_t *gb,
                                                     const struct polars_expr_t *const *exprs,
                                                     uintptr_t nexprs);

void polars_expr_destroy(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_literal_bool(bool value);

const struct polars_expr_t *polars_expr_literal_null(void);

const struct polars_expr_t *polars_expr_literal_i32(int32_t value);

const struct polars_expr_t *polars_expr_literal_i64(int64_t value);

const struct polars_expr_t *polars_expr_literal_u32(uint32_t value);

const struct polars_expr_t *polars_expr_literal_u64(uint64_t value);

const struct polars_expr_t *polars_expr_literal_f32(float value);

const struct polars_expr_t *polars_expr_literal_f64(double value);

const struct polars_error_t *polars_expr_literal_utf8(const uint8_t *s,
                                                      uintptr_t len,
                                                      const struct polars_expr_t **out);

const struct polars_error_t *polars_expr_col(const uint8_t *name,
                                             uintptr_t len,
                                             const struct polars_expr_t **out);

const struct polars_error_t *polars_expr_alias(const struct polars_expr_t *expr,
                                               const uint8_t *name,
                                               uintptr_t len,
                                               const struct polars_expr_t **out);

const struct polars_error_t *polars_expr_prefix(const struct polars_expr_t *expr,
                                                const uint8_t *name,
                                                uintptr_t len,
                                                const struct polars_expr_t **out);

const struct polars_error_t *polars_expr_suffix(const struct polars_expr_t *expr,
                                                const uint8_t *name,
                                                uintptr_t len,
                                                const struct polars_expr_t **out);

const struct polars_expr_t *polars_expr_cast(const struct polars_expr_t *expr,
                                             enum polars_value_type_t dtype);

const struct polars_expr_t *polars_expr_keep_name(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_sum(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_product(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_mean(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_median(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_min(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_max(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_arg_min(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_arg_max(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_nan_min(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_nan_max(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_floor(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_ceil(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_abs(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_cos(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_sin(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_tan(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_cosh(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_sinh(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_tanh(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_n_unique(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_unique(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_count(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_first(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_last(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_not(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_is_finite(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_is_infinite(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_is_nan(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_is_null(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_is_not_null(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_drop_nans(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_drop_nulls(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_implode(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_flatten(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_reverse(const struct polars_expr_t *expr);

const struct polars_expr_t *polars_expr_eq(const struct polars_expr_t *a,
                                           const struct polars_expr_t *b);

const struct polars_expr_t *polars_expr_lt(const struct polars_expr_t *a,
                                           const struct polars_expr_t *b);

const struct polars_expr_t *polars_expr_gt(const struct polars_expr_t *a,
                                           const struct polars_expr_t *b);

const struct polars_expr_t *polars_expr_or(const struct polars_expr_t *a,
                                           const struct polars_expr_t *b);

const struct polars_expr_t *polars_expr_xor(const struct polars_expr_t *a,
                                            const struct polars_expr_t *b);

const struct polars_expr_t *polars_expr_and(const struct polars_expr_t *a,
                                            const struct polars_expr_t *b);

const struct polars_expr_t *polars_expr_pow(const struct polars_expr_t *a,
                                            const struct polars_expr_t *b);

const struct polars_expr_t *polars_expr_add(const struct polars_expr_t *a,
                                            const struct polars_expr_t *b);

const struct polars_expr_t *polars_expr_sub(const struct polars_expr_t *a,
                                            const struct polars_expr_t *b);

const struct polars_expr_t *polars_expr_mul(const struct polars_expr_t *a,
                                            const struct polars_expr_t *b);

const struct polars_expr_t *polars_expr_div(const struct polars_expr_t *a,
                                            const struct polars_expr_t *b);

const struct polars_expr_t *polars_expr_list_lengths(const struct polars_expr_t *a);

const struct polars_expr_t *polars_expr_list_max(const struct polars_expr_t *a);

const struct polars_expr_t *polars_expr_list_min(const struct polars_expr_t *a);

const struct polars_expr_t *polars_expr_list_arg_max(const struct polars_expr_t *a);

const struct polars_expr_t *polars_expr_list_arg_min(const struct polars_expr_t *a);

const struct polars_expr_t *polars_expr_list_sum(const struct polars_expr_t *a);

const struct polars_expr_t *polars_expr_list_mean(const struct polars_expr_t *a);

const struct polars_expr_t *polars_expr_list_reverse(const struct polars_expr_t *a);

const struct polars_expr_t *polars_expr_list_unique(const struct polars_expr_t *a);

const struct polars_expr_t *polars_expr_list_unique_stable(const struct polars_expr_t *a);

const struct polars_expr_t *polars_expr_list_first(const struct polars_expr_t *a);

const struct polars_expr_t *polars_expr_list_last(const struct polars_expr_t *a);

const struct polars_expr_t *polars_expr_list_get(const struct polars_expr_t *a,
                                                 const struct polars_expr_t *b);

const struct polars_expr_t *polars_expr_list_head(const struct polars_expr_t *a,
                                                  const struct polars_expr_t *b);

const struct polars_expr_t *polars_expr_list_contains(const struct polars_expr_t *a,
                                                      const struct polars_expr_t *b);

const struct polars_expr_t *polars_expr_str_to_uppercase(const struct polars_expr_t *a);

const struct polars_expr_t *polars_expr_str_to_lowercase(const struct polars_expr_t *a);

const struct polars_expr_t *polars_expr_str_n_chars(const struct polars_expr_t *a);

const struct polars_expr_t *polars_expr_str_lengths(const struct polars_expr_t *a);

const struct polars_expr_t *polars_expr_str_explode(const struct polars_expr_t *a);

const struct polars_expr_t *polars_expr_str_starts_with(const struct polars_expr_t *a,
                                                        const struct polars_expr_t *b);

const struct polars_expr_t *polars_expr_str_ends_with(const struct polars_expr_t *a,
                                                      const struct polars_expr_t *b);

const struct polars_expr_t *polars_expr_str_contains_literal(const struct polars_expr_t *a,
                                                             const struct polars_expr_t *b);

const struct polars_expr_t *polars_expr_struct_field_by_name(const struct polars_expr_t *a,
                                                             const uint8_t *name,
                                                             uintptr_t len);

const struct polars_expr_t *polars_expr_struct_field_by_index(const struct polars_expr_t *a,
                                                              int64_t fieldidx);

const struct polars_expr_t *polars_expr_struct_rename_fields(const struct polars_expr_t *a,
                                                             const uint8_t *const *names,
                                                             const uintptr_t *lens,
                                                             uintptr_t num_names);

const struct polars_error_t *polars_series_new(const uint8_t *name,
                                               uintptr_t namelen,
                                               const uint32_t *values,
                                               uintptr_t valueslen,
                                               struct polars_series_t **out);

void polars_series_destroy(struct polars_series_t *series);

enum polars_value_type_t polars_series_type(struct polars_series_t *series);

uintptr_t polars_series_length(struct polars_series_t *series);

uintptr_t polars_series_null_count(struct polars_series_t *series);

ArrowSchema polars_series_schema(struct polars_series_t *series);

/**
 * Returns whether or not the value at index `index` is null, return false if the index is out of
 * bounds.
 */
bool polars_series_is_null(struct polars_series_t *series, uintptr_t index);

uintptr_t polars_series_name(struct polars_series_t *series, const uint8_t **out);

const struct polars_value_t *polars_series_get(struct polars_series_t *series, uintptr_t index);

const struct polars_error_t *polars_series_get_bool(struct polars_series_t *series,
                                                    uintptr_t index,
                                                    bool *out);

const struct polars_error_t *polars_series_get_u8(struct polars_series_t *series,
                                                  uintptr_t index,
                                                  uint8_t *out);

const struct polars_error_t *polars_series_get_u16(struct polars_series_t *series,
                                                   uintptr_t index,
                                                   uint16_t *out);

const struct polars_error_t *polars_series_get_u32(struct polars_series_t *series,
                                                   uintptr_t index,
                                                   uint32_t *out);

const struct polars_error_t *polars_series_get_u64(struct polars_series_t *series,
                                                   uintptr_t index,
                                                   uint64_t *out);

const struct polars_error_t *polars_series_get_i8(struct polars_series_t *series,
                                                  uintptr_t index,
                                                  int8_t *out);

const struct polars_error_t *polars_series_get_i16(struct polars_series_t *series,
                                                   uintptr_t index,
                                                   int16_t *out);

const struct polars_error_t *polars_series_get_i32(struct polars_series_t *series,
                                                   uintptr_t index,
                                                   int32_t *out);

const struct polars_error_t *polars_series_get_i64(struct polars_series_t *series,
                                                   uintptr_t index,
                                                   int64_t *out);

const struct polars_error_t *polars_series_get_f32(struct polars_series_t *series,
                                                   uintptr_t index,
                                                   float *out);

const struct polars_error_t *polars_series_get_f64(struct polars_series_t *series,
                                                   uintptr_t index,
                                                   double *out);

enum polars_value_type_t polars_value_type(struct polars_value_t *value);

void polars_value_destroy(struct polars_value_t *value);

const struct polars_error_t *polars_value_get_bool(struct polars_value_t *value, bool *out);

const struct polars_error_t *polars_value_get_u8(struct polars_value_t *value, uint8_t *out);

const struct polars_error_t *polars_value_get_u16(struct polars_value_t *value, uint16_t *out);

const struct polars_error_t *polars_value_get_u32(struct polars_value_t *value, uint32_t *out);

const struct polars_error_t *polars_value_get_u64(struct polars_value_t *value, uint64_t *out);

const struct polars_error_t *polars_value_get_i8(struct polars_value_t *value, int8_t *out);

const struct polars_error_t *polars_value_get_i16(struct polars_value_t *value, int16_t *out);

const struct polars_error_t *polars_value_get_i32(struct polars_value_t *value, int32_t *out);

const struct polars_error_t *polars_value_get_i64(struct polars_value_t *value, int64_t *out);

const struct polars_error_t *polars_value_get_f32(struct polars_value_t *value, float *out);

const struct polars_error_t *polars_value_get_f64(struct polars_value_t *value, double *out);

/**
 * Returns the value as a Series when the dtype of the value is a list.
 */
const struct polars_error_t *polars_value_list_get(struct polars_value_t *value,
                                                   struct polars_series_t **out);

const struct polars_error_t *polars_value_utf8_get(struct polars_value_t *value,
                                                   void *user,
                                                   IOCallback callback);

const struct polars_error_t *polars_value_binary_get(struct polars_value_t *value,
                                                     void *user,
                                                     IOCallback callback);

/**
 * Used to get value of of a Struct value fields.
 *
 * NOTE: The value producing the new value must outlive the value from the field.
 *
 * Safety: Values lifetimes must be valid and only support physical dtypes for now.
 */
const struct polars_error_t *polars_value_struct_get(struct polars_value_t *value,
                                                     uintptr_t fieldidx,
                                                     struct polars_value_t **out);

/**
 * Returns the element type of the provided value which must be a list.
 * The value type is PolarsValueTypeUnknown if the value is not a list
 * so makes sure it is one otherwise, you cannot differentiate between list<unkown>
 * and unkown.
 */
enum polars_value_type_t polars_value_list_type(struct polars_value_t *value);
