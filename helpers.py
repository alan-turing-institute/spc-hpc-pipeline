def query_yes_no(question: str, default: str = "yes") -> str:
    """
    Prompts the user for yes/no input, displaying the specified question text.

    :param str question: The text of the prompt for input.
    :param str default: The default if the user hits <ENTER>. Acceptable values
    are 'yes', 'no', and None.
    :return: 'yes' or 'no'
    """
    valid = {'y': 'yes', 'n': 'no'}
    if default is None:
        prompt = ' [y/n] '
    elif default == 'yes':
        prompt = ' [Y/n] '
    elif default == 'no':
        prompt = ' [y/N] '
    else:
        raise ValueError(f"Invalid default answer: '{default}'")
    choice = default

    while 1:
        user_input = input(question + prompt).lower()
        if not user_input:
            break
        try:
            choice = valid[user_input[0]]
            break
        except (KeyError, IndexError):
            print("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")

    return choice


def sanitize_container_name(orig_name):
    """
    only allowed alphanumeric characters and dashes.
    """
    sanitized_name = ""
    previous_character = None
    for character in orig_name:
        if not re.search("[-a-zA-Z\d]", character):
            if not previous_character == "-":
                sanitized_name += "-"
                previous_character = "-"
            else:
                continue
        else:
            sanitized_name += character.lower()
            previous_character = character
    if "\\" in sanitized_name:
        sanitized_name = sanitized_name.replace("\\","/")

    return sanitized_name