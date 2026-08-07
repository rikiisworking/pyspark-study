# Window functions 8/8

User completed `07_window` with all cases green: row_number (deterministic tie-break), rank vs dense_rank on amount ties, lag/lead by day, full-partition sum, running sum via default ordered frame, top-1 per region via row_number + filter.

Implications: optional track item “window” is fluent. Mission core DF chain + annotate-rows path covered. Optional next: read/write + nulls.
