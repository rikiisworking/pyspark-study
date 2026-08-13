# Union / unionByName 8/8

User completed `19_union` green: positional `union` (including the shuffled-seat trap), `unionByName`, `allowMissingColumns=True`, `distinct`, chain three frames, filter after stack, tag `src` before union.

No stall. ex1/ex2 extra `.select` after union is a no-op (columns already those names).

Implications: stretch 11 demonstrated. Offer union + pipeline mix before the next new API (broadcast / pivot / array HOFs).
