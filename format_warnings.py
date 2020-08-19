    #formats warming method to not double print for user and allows string formatting
def format_Warning(message, category, filename, lineno, line=''):
    return str(filename) + ':' + str(lineno) + ': ' + category.__name__ + ': ' +str(message) + '\n'

    