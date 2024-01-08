def swap_1_and_0(n):
    if n == 1: n = 0
    else : n = 1
    print(n)

def main():
    n = 5
    # for i in range(5):
    #     for j in range(i+1):
    #         print("*", end = "")
    #     print()
    j = 1
    for i in range(n):
        print("*" * j, end = "")
        j = j + 1
        print()



if __name__ == '__main__':
    main()
