package tests;

import global.AttrOperator;
import global.AttrType;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;
import global.AttrType;

public class OperationUtils {
    private static boolean isUnix() {
        String os = System.getProperty("os.name").toLowerCase();
        return (os.indexOf("nix") >= 0 || os.indexOf("nux") >= 0 || os.indexOf("aix") >= 0);
    }

    public static String dbPath(String columnDB){
        String path;
        if(isUnix()) {
            path = "/tmp/";
        } else {
            path = "C:\\Windows\\Temp\\";
        }
        return path + columnDB + ".minibase-db";
    }

    public static CondExpr[] processRawConditionExpression(String expression) {
        // Sample input
        // String expression = "([columnarTable1.A = 'RandomTextHere'] v [columnarTable1.B > 2]) ^ ([columnarTable1.C = columnarTable1.D])"
        CondExpr[] condExprs;

        if (expression.length() == 0) {
            condExprs = new CondExpr[1];
            condExprs[0] = null;

            return condExprs;
        }

        String[] andExpressions = expression.split(" \\^ ");
        condExprs = new CondExpr[andExpressions.length + 1];
        for (int i = 0; i < andExpressions.length; i++) {
            String temp = andExpressions[i].replace("(", "");
            temp = temp.replace(")", "");
            String[] orExpressions = temp.split(" v ");

            condExprs[i] = new CondExpr();
            CondExpr conditionalExpression = condExprs[i];
            for (int j = 0; j < orExpressions.length; j++) {
                String singleExpression = orExpressions[j].replace("[", "");
                singleExpression = singleExpression.replace("]", "");
                String[] expressionParts = singleExpression.split(" ");
                String stringOperator = expressionParts[1];
                String attributeValue = expressionParts[2];

                conditionalExpression.type1 = new AttrType(AttrType.attrSymbol);
                conditionalExpression.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                conditionalExpression.op = getOperatorForString(stringOperator);
                if (isInteger(attributeValue)) {
                    conditionalExpression.type2 = new AttrType(AttrType.attrInteger);
                    conditionalExpression.operand2.integer = Integer.parseInt(attributeValue);
                } else if (isString(attributeValue)) {
                    conditionalExpression.type2 = new AttrType(AttrType.attrString);
                    conditionalExpression.operand2.string = attributeValue.replace("'","");
                } else {
                    conditionalExpression.type2 = new AttrType(AttrType.attrSymbol);
                    conditionalExpression.operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                }

                if (j == orExpressions.length - 1) {
                    conditionalExpression.next = null;
                } else {
                    conditionalExpression.next = new CondExpr();
                    conditionalExpression = conditionalExpression.next;
                }
            }
        }
        condExprs[andExpressions.length] = null;

        return condExprs;
    }
    public static CondExpr[] processRawConditionExpression(String expression, String[] targetColumns) throws Exception {
        // Sample input
        // String expression = "([columnarTable1.A = 'RandomTextHere'] v [columnarTable1.B > 2]) ^ ([columnarTable1.C = columnarTable1.D])"
        CondExpr[] condExprs;

        if (expression.length() == 0) {
            condExprs = new CondExpr[1];
            condExprs[0] = null;

            return condExprs;
        }

        String[] andExpressions = expression.split(" \\^ ");
        condExprs = new CondExpr[andExpressions.length + 1];
        for (int i = 0; i < andExpressions.length; i++) {
            String temp = andExpressions[i].replace("(", "");
            temp = temp.replace(")", "");
            String[] orExpressions = temp.split(" v ");

            condExprs[i] = new CondExpr();
            CondExpr conditionalExpression = condExprs[i];
            for (int j = 0; j < orExpressions.length; j++) {
                String singleExpression = orExpressions[j].replace("[", "");
                singleExpression = singleExpression.replace("]", "");
                String[] expressionParts = singleExpression.split(" ");
                String attributeName = getAttributeName(expressionParts[0]);
                String stringOperator = expressionParts[1];
                String attributeValue = expressionParts[2];

                conditionalExpression.type1 = new AttrType(AttrType.attrSymbol);
                conditionalExpression.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), getColumnPositionInTargets(attributeName, targetColumns) + 1);
                conditionalExpression.op = getOperatorForString(stringOperator);
                if (isInteger(attributeValue)) {
                    conditionalExpression.type2 = new AttrType(AttrType.attrInteger);
                    conditionalExpression.operand2.integer = Integer.parseInt(attributeValue);
                } else if (isString(attributeValue)) {
                    conditionalExpression.type2 = new AttrType(AttrType.attrString);
                    conditionalExpression.operand2.string = attributeValue.replace("'","");
                } else {
                    conditionalExpression.type2 = new AttrType(AttrType.attrSymbol);
                    String name = getAttributeName(attributeValue);
                    conditionalExpression.operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer), getColumnPositionInTargets(name, targetColumns) + 1);
                }

                if (j == orExpressions.length - 1) {
                    conditionalExpression.next = null;
                } else {
                    conditionalExpression.next = new CondExpr();
                    conditionalExpression = conditionalExpression.next;
                }
            }
        }
        condExprs[andExpressions.length] = null;

        return condExprs;
    }

    private static Boolean isString(String value) {
        if (value.charAt(0) == '\'' && value.charAt(value.length() - 1) == '\'') {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    public static String getAttributeName(String value) {
        String[] val = value.split("\\.");
        return val.length == 2? val[1]:val[0];
    }

    private static AttrOperator getOperatorForString(String operator) {
        switch (operator) {
            case "=":
                return new AttrOperator(AttrOperator.aopEQ);
            case ">":
                return new AttrOperator(AttrOperator.aopGT);
            case "<":
                return new AttrOperator(AttrOperator.aopLT);
            case "!=":
                return new AttrOperator(AttrOperator.aopNE);
            case "<=":
                return new AttrOperator(AttrOperator.aopLE);
            case ">=":
                return new AttrOperator(AttrOperator.aopGE);
        }

        return null;
    }

    public static boolean isInteger(String s) {
        try {
            Integer intValue = Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    public static int getColumnPositionInTargets(String columnName, String[] targetColumns) throws Exception {
        System.out.println("Column Name " + columnName);
        System.out.println();
        for(int i = 0; i < targetColumns.length; i++) {
            System.out.println("target column " + i + targetColumns[i]);
        }
        for(int i = 0; i < targetColumns.length; i++){
            String targetColumn = targetColumns[i].split("\\.")[1];
            String[] columnNames = columnName.split("\\.");
            if (columnNames.length == 2) {
                columnName = columnNames[1];
            } else {
                columnName = columnNames[0];
            }
            if(columnName.equals(targetColumn))
                return i;
        }
        throw new Exception(columnName + " not found in targets");
    }
}