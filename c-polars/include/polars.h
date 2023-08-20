#ifndef POLARS_H
#define POLARS_H

#include <stdlib.h>

typedef struct polars_error_t polars_error_t;

typedef struct polars_series_t polars_series_t;

typedef struct polars_dataframe_t polars_dataframe_t;

typedef struct polars_lazy_frame_t polars_lazy_frame_t;

typedef struct polars_expr_t polars_expr_t;

unsigned polars_error_message(polars_error_t *err, char **str);

void polars_error_destroy(polars_error_t *err);

void polars_dataframe_destroy(polars_dataframe_t *df);

void polars_lazy_frame_destroy(polars_lazy_frame_t *df);

polars_lazy_frame_t *polars_dataframe_lazy(polars_dataframe_t *df);

polars_error_t *polars_lazy_frame_collect(polars_lazy_frame_t *df,
                                          polars_dataframe_t **out);

polars_error_t *polars_dataframe_read_parquet(char *path, unsigned pathlen,
                                              polars_dataframe_t **out);

void polars_dataframe_show(polars_dataframe_t *, void *ref, void *callback);

void polars_lazy_frame_select(polars_lazy_frame_t *df,
                              polars_expr_t **exprs_ptrs, int exprs_ptrslen);

polars_error_t *polars_lazy_frame_fetch(polars_lazy_frame_t *df, unsigned n,
                                        polars_dataframe_t **out);

void polars_lazy_frame_filter(polars_lazy_frame_t *df, polars_expr_t *expr);

polars_lazy_frame_t *polars_lazy_frame_clone(polars_lazy_frame_t *df);

void polars_expr_destroy(polars_expr_t *expr);

polars_expr_t *polars_expr_literal_i32(int v);

polars_expr_t *polars_expr_literal_i64(long v);

polars_expr_t *polars_expr_literal_u32(unsigned v);

polars_expr_t *polars_expr_literal_u64(unsigned long v);

polars_expr_t *polars_expr_literal_bool(char v);

polars_expr_t *polars_expr_literal_null();

polars_error_t *polars_expr_literal_utf8(char *s, size_t slen,
                                         polars_expr_t **out);

polars_error_t *polars_expr_col(char *name, unsigned namelen,
                                polars_expr_t **expr);

polars_error_t *polars_expr_alias(polars_expr_t *expr, char *alias,
                                  size_t aliaslen, polars_expr_t **out);

unsigned polars_series_length(polars_series_t *series);

polars_error_t *polars_series_get_u32(polars_series_t *series, unsigned index,
                                      unsigned *out);

polars_error_t *polars_series_new(char *name, unsigned long namelen,
                                  unsigned *values, unsigned long valueslen,
                                  polars_series_t **ptr);

unsigned polars_series_name(polars_series_t *series, char **ptr);

#endif // POLARS_H
