"""Fixed Intel/Morrie150 variant; same no-submit qualification guards."""
import ci_go150_qualify as q
if __name__=='__main__':
 q.SOURCE='d55ea0b63359beb3194c2654b90c96fcfbabb07a'
 q.INPUT_SHA='0dc142489f23651d78ecac19c7a6632351d840ef216cc590c692f74af222a6a1'
 q.BINARY_SHA='82f3f339dba5813b9151acb1257a3036436d83e61cdc434cf156012e5e0f4124'
 q.PREVIOUS_UNRESOLVED=None
 q.ORIGINAL='609d2fbb-7f2e-4d65-bbcf-7746d1784319'
 q.ARCH='x86_64'
 q.OUTPUT_NAME='Everything-Go-0.2.150-morrie-ci-qualified-x64.zip'
 raise SystemExit(q.main()or 0)
