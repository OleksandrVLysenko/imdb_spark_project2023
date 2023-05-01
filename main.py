from task1 import task1

def main():
    # Task 1
    path_to_save1 = "task_results\\Task1\\"
    path1 = r"imdb_data\title.akas.tsv.gz"
    task1(path1, path_to_save1)

    # # Task 2
    # path_to_save2 = "task_results\\Task2\\"
    # path2 = r"imdb_data\name.basics.tsv.gz"
    # task2(path2, path_to_save2)

if __name__ == "__main__":
    main()
