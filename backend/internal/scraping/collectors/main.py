import  time
from    portalzuk   import  main

if  __name__   ==   "__main__":
    start=time.time()
    main()
    end=time.time()
    print(f"Tempo total de execução: {end-start:.2f} segundos")
