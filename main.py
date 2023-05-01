from task1 import task1
from task2 import task2
from task3 import task3
from task4 import task4

def main():
    # # Task 1
    # path_to_save1 = "task_results\\Task1\\"
    # path1 = r"imdb_data\title.akas.tsv.gz"
    # task1(path1, path_to_save1)

    # # Task 2
    # path_to_save2 = "task_results\\Task2\\"
    # path2 = r"imdb_data\name.basics.tsv.gz"
    # task2(path2, path_to_save2)

    # # Task 3
    # path_to_save3 = "task_results\\Task3\\"
    # path3 = r"imdb_data\title.basics.tsv.gz"
    # task3(path3, path_to_save3)

    # Task 4
    path_to_save4 = "task_results\\Task4\\"
    path4 = r"imdb_data\title.principals.tsv.gz"
    path4add_name = r"imdb_data\name.basics.tsv.gz"
    path4add_title = r"imdb_data\title.basics.tsv.gz"
    task4(path4, path_to_save4, path4add_name, path4add_title)


if __name__ == "__main__":
    main()
