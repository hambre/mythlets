#!/usr/bin/env python

import sys, os, os.path
sys.path.append("/usr/bin")
import cppcheckdata

def printTokens(data):
    for token in data.tokenlist:
        print(token.str)

def printConfigurations(data):
    for cfg in data.configurations:
        print('cfg: ' + cfg.name)
        code = ''
        for token in cfg.tokenlist:
            code = code + token.str + ' '
        print('    ' + code)

def printFunctions(data):
    for cfg in data.configurations:
        print('cfg: ' + cfg.name)
        for func in cfg.functions:
            print(func.Id + ': ' + func.name)

def findToken(data, tokenStr, fileName=None, lineNo=None):
    for cfg in data.configurations:
        for token in cfg.tokenlist:
            # token string must match
            if token.str != tokenStr:
                continue
            # line number must match if it was specified
            if lineNo and token.linenr != lineNo:
                continue
            # file name must match if it was specified
            if fileName and not token.file.endswith(fileName):
                continue
            return token
    return None

def findTokenById(data, tokenId, fileName=None):
    for cfg in data.configurations:
        for token in cfg.tokenlist:
            if token.Id != tokenId:
                continue
            if fileName == None or not token.file.endswith(fileName):
                continue
            return token
    return None

def printToken(token):
    print('id: ' + token.Id)
    print('str: ' + token.str)
    print('line: ' + str(token.linenr))
    if token.file != None:
        print('file: ' + token.file)
    if token.function != None:
        print('function: ' + token.function.Id)

def findNextFuncToken(data, startToken):
    funcName = startToken.str
    fileName = startToken.file;
    lineNo = startToken.linenr
    token = startToken
    while token:
        if token.file != fileName:
            return None
        if token.linenr != lineNo:
            return None
        if token.str == funcName and token.next.str == '(':
            return token
        token = token.next
    return None

# find function argument tokens
def findFunctionArgTokens(data, funcName, fileName=None, lineNo=None):
    funcToken = findToken(data, funcName, fileName, lineNo)
    funcToken = findNextFuncToken(data, funcToken)
    if not funcToken:
        sys.stderr.write('Could not find function ' + funcName + '\n')
        return None
    arguments = []
    tokens = []
    nextToken = funcToken.next
    while nextToken.next:
        nextToken = nextToken.next
        if nextToken.str == ',':
            if len(tokens) == 1:
                arguments.append(tokens[0])
            else:
                arguments.append(None)
            tokens.clear()
            continue
        elif nextToken.str == ')':
            if len(tokens) == 1:
                arguments.append(tokens[0])
            else:
                arguments.append(None)
            tokens.clear()
            break;
        else:
            tokens.append(nextToken)
    return arguments

# find all tokens corresponding to the specified variable
def findVariableTokens(data, variable):
    varTokens = []
    for cfg in data.configurations:
        for token in cfg.tokenlist:
            if not token.variable:
                continue
            if token.variable == variable:
                varTokens.append(token)
    return varTokens

# create a dump file for the specified source file
def createDump(mythBaseDir, mythSrcFile):
    args = []
    args.append('cppcheck')
    args.append('--dump')
    args.append('--quiet')
    args.append('-DMUI_API')
    args.append('-I' + os.path.join(mythBaseDir, 'mythtv/libs/libmythui/'))
    args.append(mythSrcFile)
    res = os.spawnvp(os.P_WAIT, 'cppcheck', args)
    if res != 0:
        sys.stderr.write('Running cppcheck failed with error ' + res + '\n')
        return None
    dumpFile = mythSrcFile + '.dump'
    if not os.path.exists(dumpFile):
        sys.stderr.write('Dump file could not be created\n')
        return None
    return dumpFile

def findValues(data, fileName, token):
    values = []
    if not token:
        return values
    elif token.isString:
        # token is a string literal
        values.append(token.str.strip('"'))
    elif token.values:
        # token is a variable with detected values
        for value in token.values:
            if value.tokvalue:
                v = findTokenById(data, value.tokvalue, fileName)
                if v:
                    values.append(v.str.strip('"'))
    elif token.variable:
        # find assignments to variable
        varTokens = findVariableTokens(data, token.variable)
        for vt in varTokens:
            if not vt.next:
                continue
            if vt.next.isAssignmentOp:
                # assignment
                if vt.next.astOperand2.str == '[':
                    # assignment from array
                    values.extend(findValues(data, fileName, vt.next.astOperand2.previous))
                elif vt.next.astOperand2.str == '?':
                    # assignment via ternary operator
                    colonToken = vt.next.astOperand2.astOperand2
                    values.extend(findValues(data, fileName, colonToken.astOperand1))
                    values.extend(findValues(data, fileName, colonToken.astOperand2))
                else:
                    values.extend(findValues(data, fileName, vt.next.astOperand2))
            elif vt.next.str == '(' and vt.next.link and vt.next.link.str == ')':
                # constructor
                values.extend(findValues(data, fileName, vt.next.astOperand2))
            elif vt.next.str == '[' and vt.next.link and vt.next.link.str == ']':
                # check for array
                assignToken = vt.next.link.next
                if assignToken.isAssignmentOp and assignToken.next.str == '{':
                    # initializer list
                    nextToken = assignToken.next
                    while nextToken.next:
                        nextToken = nextToken.next
                        if nextToken.str == ',':
                            continue
                        elif nextToken.str == '}':
                            break;
                        values.extend(findValues(data, fileName, nextToken))
                else:
                    # normal assignment
                    values.extend(findValues(data, fileName, assignToken.astOperand2))
    return values

def findFunctionArgs(dumpFile, functionName, lineNo=None):
    # determine parsed file name form dump file name
    fileName = os.path.splitext(os.path.basename(dumpFile))[0]
    # parse the dump file
    data = cppcheckdata.parsedump(dumpFile)
    # find function argument tokens
    argTokens = findFunctionArgTokens(data, functionName, fileName, lineNo)
    if not argTokens:
        sys.stderr.write('Could not find function arguments\n')
        return []
    argStrings = []
    for t in argTokens:
        argStrings.append(findValues(data, fileName, t))
    return argStrings

def main():
    mythSrcDir = '/mnt/data/devel/mythtv-fixes-0.27/'
    #mythSrcFile = '/mnt/data/devel/mythtv-fixes-0.27/mythplugins/mythnetvision/mythnetvision/nettree.cpp'
    #mythSrcFile = '/mnt/data/devel/mythtv-fixes-0.27/mythtv/programs/mythfrontend/videodlg.cpp'
    #mythSrcFile = '/mnt/data/devel/mythtv-fixes-0.27/mythtv/libs/libmythtv/subtitlescreen.cpp'
    #mythSrcFile = '/mnt/data/devel/mythtv-fixes-0.27/mythtv/libs/libmythtv/osd.cpp'
    mythSrcFile = '/mnt/data/devel/mythtv/mythplugins/mythmusic/mythmusic/lyricsview.cpp'
    functionName = 'LoadWindowFromXML'
    #functionName = 'MythOSDWindow'
    #mythSrcFile = '/mnt/data/devel/mythtv-fixes-0.27/mythtv/programs/mythfrontend/scheduleeditor.cpp'
    #functionName = 'CreateEditChild'
    dumpFile = createDump(mythSrcDir, mythSrcFile)
    #dumpFile = '/mnt/data/devel/mythtv-fixes-0.27/mythplugins/mythnetvision/mythnetvision/nettree.cpp.dump'
    if not dumpFile:
        return 1
    funcArgs = findFunctionArgs(dumpFile, functionName)
    #os.remove(dumpFile)
    print(funcArgs)

if __name__ == "__main__":
    main()
