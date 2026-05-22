"""run using `uv run pytest tests/test_sorted_set.py`"""

from app.red_black_tree import Node, RedBlackTree, build_tree_from_dict

"""
a red black tree is a BST i.e. inorder traversal should return a sorted list
invariants:
0. root must be black (for the sake of analysis)
1. black height must be the same
2. red nodes cannot have red children
3. (derived) if a node has 1 child, it must be red

start with a tree of ints, then generalise

"""

########################################################
## INVARIANT CHECKS
########################################################


def check_is_bst(node: Node) -> bool:
    inorder = inorder_tree(node)
    return inorder == sorted(inorder)


def is_valid_black_height(node: Node | None) -> bool:
    valid_height = True

    def black_height_visit(node: Node | None) -> int:
        """Returns the number of black nodes in the path starting from current node to leaves"""
        nonlocal valid_height
        if not valid_height:
            # short circuit, value doesn't matter
            return 0
        if node is None:
            return 1
        left = black_height_visit(node.left)
        right = black_height_visit(node.right)
        if left != right:
            valid_height = False
        return left + (node.colour == "black")

    black_height_visit(node)
    return valid_height


def has_no_red_red(node: Node | None) -> bool:
    """Checks if there are no violations of the red parent red child rule"""
    if node is None:
        return True
    if node.colour == "red" and (
        (node.left and node.left.colour == "red")
        or (node.right and node.right.colour == "red")
    ):
        return False
    return has_no_red_red(node.left) and has_no_red_red(node.right)


def is_all_parent_child_consistent(node: Node | None) -> bool:
    if node is None:
        return True
    left = True
    right = True
    if node.left:
        left &= node.left.parent == node
        left &= is_all_parent_child_consistent(node.left)
    if node.right:
        right &= node.right.parent == node
        right &= is_all_parent_child_consistent(node.right)
    return left and right


def check_is_rbtree(tree: RedBlackTree, debug: bool = False) -> bool:
    if tree.root is None:
        if debug:
            print("tree.root is None")
        return True
    is_bst = check_is_bst(tree.root)
    is_root_black = tree.root.colour == "black"
    is_black_height_valid = is_valid_black_height(tree.root)
    no_red_red = has_no_red_red(tree.root)
    all_parent_childs_valid = is_all_parent_child_consistent(tree.root)
    if debug:
        if not is_bst:
            print("not is_bst")
        if not is_root_black:
            print("not is_root_black")
        if not is_black_height_valid:
            print("not is_black_height_valid")
        if not no_red_red:
            print("not no_red_red")
        if not all_parent_childs_valid:
            print("not all_parent_childs_valid")
    return (
        is_bst
        and is_root_black
        and is_black_height_valid
        and no_red_red
        and all_parent_childs_valid
    )


def inorder_tree(tree: Node) -> list[int]:
    inorder = []

    def visit(node):
        if node is not None:
            visit(node.left)
            inorder.append(node.val)
            visit(node.right)

    visit(tree)
    return inorder


########################################################
## TREE BUILDING UTILS
########################################################


def build_tree1() -> RedBlackTree:
    tree1 = RedBlackTree()
    nodes = [
        Node(1, "black"),
        Node(0, "black"),
        Node(3, "red"),
        Node(2, "black"),
        Node(4, "black"),
    ]
    tree1.root = nodes[0]
    nodes[0].left = nodes[1]
    nodes[0].right = nodes[2]
    nodes[2].left = nodes[3]
    nodes[2].right = nodes[4]

    nodes[0].parent = tree1.root
    nodes[1].parent = nodes[0]
    nodes[2].parent = nodes[0]
    nodes[3].parent = nodes[2]
    nodes[4].parent = nodes[2]
    return tree1


def test_valid_tree1():
    tree = build_tree1()
    root = tree.root
    assert root is not None
    is_valid_bst = check_is_bst(root)
    is_valid_rbtree = check_is_rbtree(tree)
    print(f"{is_valid_bst=}")
    print(f"{is_valid_rbtree=}")
    assert is_valid_bst
    assert is_valid_rbtree


# def test_rbtree_insert_success():
#     pass


# def test_rbtree_delete_success():
#     pass

########################################################
## ROTATION TESTS
########################################################
"""
edge cases:
1. node is root
2. no grandchild (child.left for left_rotate in particular. so the inserted node can be child.right)
test on a tree that is invalid, by manually inserting a node on a balanced tree
start with testing after insertion

things to check: is valid BST, all parent child connections
"""


def test_rbtree_right_rotate_after_insert_zig_zig_10b_success():
    tree = build_tree_from_dict({5: {"r": {(7, "red"): {"l": (6, "red")}}}})
    assert tree.root is not None and tree.root.right is not None
    assert check_is_bst(tree.root) and is_all_parent_child_consistent(tree.root)
    tree.right_rotate(tree.root.right)
    assert check_is_bst(tree.root) and is_all_parent_child_consistent(tree.root)
    assert tree == build_tree_from_dict({5: {"r": {(6, "red"): {"r": (7, "red")}}}})


def test_rbtree_right_rotate_after_insert_zig_zig_01b_success():
    tree = build_tree_from_dict({5: {"r": {(7, "red"): {"l": (6, "red")}}}})
    assert tree.root is not None and tree.root.right is not None
    assert check_is_bst(tree.root) and is_all_parent_child_consistent(tree.root)
    tree.right_rotate(tree.root.right)
    assert check_is_bst(tree.root) and is_all_parent_child_consistent(tree.root)
    assert tree == build_tree_from_dict({5: {"r": {(6, "red"): {"r": (7, "red")}}}})


def test_rbtree_right_rotate_after_insert_zig_zig_11b_success():
    tree = build_tree_from_dict({3: {"l": {(2, "red"): {"l": (1, "red")}}}})
    assert tree.root is not None
    assert check_is_bst(tree.root) and is_all_parent_child_consistent(tree.root)
    tree.right_rotate(tree.root)
    assert check_is_bst(tree.root) and is_all_parent_child_consistent(tree.root)
    assert tree == build_tree_from_dict({2: {"l": (1, "red"), "r": (3, "red")}})


just_after_insertion_zig_zig_tree = {
    2: {"l": 1, "r": {(4, "red"): {"l": (3), "r": (5, "red")}}}
}
jaizzt_right_tree = {
    10: {"l": {(8, "red"): {"l": 7, "r": 9}}, "r": {(12, "red"): {"l": 11, "r": 13}}}
}
fixed_after_insertion_zig_zig_tree = {
    (4, "red"): {"l": {2: {"l": 1, "r": 3}}, "r": (5, "red")}
}


def test_rbtree_left_rotate_after_insert_zig_zig_10b_success():
    tree = build_tree_from_dict(just_after_insertion_zig_zig_tree)
    assert tree.root is not None
    assert check_is_bst(tree.root) and is_all_parent_child_consistent(tree.root)
    tree.left_rotate(tree.root)
    assert check_is_bst(tree.root) and is_all_parent_child_consistent(tree.root)
    assert tree == build_tree_from_dict(fixed_after_insertion_zig_zig_tree)


def test_rbtree_left_rotate_after_insert_zig_zig_00b_success():
    tree = build_tree_from_dict(
        {
            6: {
                "l": just_after_insertion_zig_zig_tree,
                "r": jaizzt_right_tree,
            }
        }
    )
    assert tree.root is not None and tree.root.left is not None
    assert check_is_bst(tree.root) and is_all_parent_child_consistent(tree.root)
    tree.left_rotate(tree.root.left)
    assert check_is_bst(tree.root) and is_all_parent_child_consistent(tree.root)
    expected = build_tree_from_dict(
        {
            6: {
                "l": {(4, "red"): {"l": {2: {"l": 1, "r": 3}}, "r": (5, "red")}},
                "r": jaizzt_right_tree,
            }
        }
    )
    assert tree == expected


def test_rbtree_left_rotate_after_insert_zig_zig_01b_success():
    tree = build_tree_from_dict(
        {
            6: {
                "l": {2: {"l": 1, "r": {3: {"r": (4, "red")}}}},
                "r": {8: {"l": 7, "r": 9}},
            }
        }
    )
    assert tree.root is not None and tree.root.left is not None
    assert check_is_bst(tree.root) and is_all_parent_child_consistent(tree.root)
    tree.left_rotate(tree.root.left)
    assert check_is_bst(tree.root) and is_all_parent_child_consistent(tree.root)
    expected = build_tree_from_dict(
        {
            6: {
                "l": {3: {"l": {2: {"l": 1}}, "r": (4, "red")}},
                "r": {8: {"l": 7, "r": 9}},
            }
        }
    )
    assert tree == expected


def test_rbtree_left_rotate_after_insert_zig_zig_11b_success():
    tree = build_tree_from_dict({2: {"l": 1, "r": {3: {"r": (4, "red")}}}})
    assert tree.root is not None
    assert check_is_bst(tree.root) and is_all_parent_child_consistent(tree.root)
    tree.left_rotate(tree.root)
    assert check_is_bst(tree.root) and is_all_parent_child_consistent(tree.root)
    expected = build_tree_from_dict({3: {"l": {2: {"l": 1}}, "r": (4, "red")}})
    assert tree == expected


jaizzt = build_tree_from_dict(just_after_insertion_zig_zig_tree)
t1 = build_tree_from_dict(
    {
        6: {
            "l": just_after_insertion_zig_zig_tree,
            "r": jaizzt_right_tree,
        }
    }
)
t2 = build_tree_from_dict(
    {
        6: {
            "l": {(1, "red"): {"l": {2: {"l": 3, "r": 4}}, "r": (5, "red")}},
            "r": jaizzt_right_tree,
        }
    }
)
t0 = RedBlackTree()
[t0.insert(i) for i in range(1, 7)]
t0.insert(7)
