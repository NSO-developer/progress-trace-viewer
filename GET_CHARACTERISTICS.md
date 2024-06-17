# LSA Characteristics

## TODO

 - cfs northbound
    - push service
       - ok
       - error due to device lock (sync-from or check-sync)
       - error due to dry-run
    - dry-run


## Northbound operations

- Get top northbound operations
    - CFS dry-runs (See next)
        - Transaction id, trace id and timestamp.
    - CFS transactions (Must filter out dry-runs)
        - Transaction id, trace id and timestamp.
    - Get RFS check-sync and sync-from actions
        - Transaction id, trace id and timestamp.

- Get service characteristics
    - 









- Get device characteristics algorithm
    - CFS holding transaction lock
      - CFS apply transaction (same TID, TRID)
        - CFS push configuration (same TID, TRID)
          - RFS apply transaction (within CFS push configuration)
            - RFS push configuration
              - device name
              - connect



## 