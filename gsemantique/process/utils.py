def change_dtype(obj, track_types=True, dtype="float32", na_value=None, **kwargs):
    # self-contained imports
    import semantique as sq

    # convert dtype
    newobj = obj.copy(deep=True)
    newobj.values = newobj.astype(dtype)
    # track value types
    if track_types:
        newobj.sq.value_type = sq.processor.types.get_value_type(newobj)
    return newobj


def update_na(obj, track_types=True, na_value=None, **kwargs):
    """
    Updates NA values by...
        * converting existing NA values to specified ones
        * persisting the NA value as part of the rio metadata

    Note that it doesn't turn existing non-NA values into NA values.
    For this functionality see the verb `assign`.
    """
    # self-contained imports
    import numpy as np
    import semantique as sq

    # update NA values
    newobj = obj.copy(deep=True)
    na_value = eval(na_value) if isinstance(na_value, str) else na_value
    if newobj.rio.nodata is None:
        nodata = np.nan if newobj.dtype.kind == "f" else None
        if na_value is not None:
            if nodata is np.nan:
                newobj.values = np.where(
                    np.isnan(newobj.values), na_value, newobj.values
                )
            else:
                newobj.values = np.where(
                    newobj.values == nodata, na_value, newobj.values
                )
        else:
            na_value = nodata
        newobj = newobj.rio.write_nodata(na_value)
    else:
        if na_value is not None:
            nodata = newobj.rio.nodata
            if nodata is np.nan:
                newobj.values = np.where(
                    np.isnan(newobj.values), na_value, newobj.values
                )
            else:
                newobj.values = np.where(
                    newobj.values == nodata, na_value, newobj.values
                )
            newobj = newobj.rio.write_nodata(na_value)
    # track value types
    if track_types:
        newobj.sq.value_type = sq.processor.types.get_value_type(newobj)
    return newobj
