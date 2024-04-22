package columnar;

import bitmap.BM;
import bitmap.BitMapFile;
import global.AttrType;
import global.TID;
import heap.Tuple;
import iterator.*;

import java.util.*;
import java.util.stream.Collectors;

public class ColumnarBitmapEquiJoins {
    private final Columnarfile leftColumnarFile;
    private final Columnarfile rightColumnarFile;
    private int offset1;
    private int offset2;
    private List<String> joinConditions = new ArrayList<>();
    // contains two lists with R1 and R2 offsets
    private List<List<Integer>> offsets = new ArrayList<>();
    // need to change to ValueClass
    private final boolean isCompressed = false;

    public ColumnarBitmapEquiJoins(
            AttrType[] in1,
            int len_in1,
            short[] t1_str_sizes,
            AttrType[] in2,
            int len_in2,
            short[] t2_str_sizes,
            int amt_of_mem,
            String leftColumnarFileName,
            int leftJoinField,
            String rightColumnarFileName,
            int rightJoinField,
            FldSpec[] proj_list,
            int n_out_flds,
            CondExpr[] joinExp,
            CondExpr[] innerExp,
            CondExpr[] outerExp, AttrType[] opAttr) throws Exception {


        assert innerExp.length == 1;
        assert outerExp.length == 1;

        leftColumnarFile = new Columnarfile(leftColumnarFileName);
        rightColumnarFile = new Columnarfile(rightColumnarFileName);

        //
        offsets.add(new ArrayList<>());
        offsets.add(new ArrayList<>());

        List<HashSet<String>> uniSet = getUniqueSetFromJoin(joinExp, leftColumnarFile, rightColumnarFile);


        List<List<String>> positions = new ArrayList<>();

        for(int i =0; i < uniSet.size(); i++) {
            HashSet<String> unisets = uniSet.get(i);


            List<String> predicatePositions = new ArrayList<>();
            for(String eachUniqueSet: unisets ) {

                AttrType attrType = leftColumnarFile.getAttrtypeforcolumn(i);

                String leftBMname = leftColumnarFile.getBMName(i, attrType, eachUniqueSet, isCompressed);

                BitMapFile leftBitMapfile = new BitMapFile(leftBMname);

                String rightBMname = rightColumnarFile.getBMName(i, attrType, eachUniqueSet, isCompressed);
                BitMapFile rightBitMapfile = new BitMapFile(rightBMname);

                BitSet leftBitMaps = BM.getBitMap(leftBitMapfile.getHeaderPage());
                BitSet rightBitMaps = BM.getBitMap(rightBitMapfile.getHeaderPage());


                List<Integer> r1Positions = new ArrayList<>();
                List<Integer> r2Positions = new ArrayList<>();

                for(int k = 0; k < leftBitMaps.size(); k++) {
                    if(leftBitMaps.get(k)) {
                        r1Positions.add(k);
                    }
                }

                for(int k = 0; k < rightBitMaps.size(); k++) {
                    if(rightBitMaps.get(k)) {
                        r2Positions.add(k);
                    }
                }

                List<List<Integer>> entries = new ArrayList<>();
                entries.add(r1Positions);
                entries.add(r2Positions);
                List<List<Integer>> lists = nestedLoop(entries);


                for (List<Integer> list : lists) {

                    predicatePositions.add(list.get(0) + "#" + list.get(1));
                }
            }

            positions.add(predicatePositions);

        }

        HashSet<String> resultTillNow = new HashSet<>(positions.get(0));

        List<HashSet<String>> newList = new ArrayList<>();

        for(int i = 0, j = 1; i < joinConditions.size(); i++, j++) {
            if(joinConditions.get(i).equals("OR")) {
                resultTillNow.addAll(positions.get(j));
            } else {
                newList.add(resultTillNow);
                resultTillNow = new HashSet<>(positions.get(j));
            }
        }
        newList.add(resultTillNow);

        resultTillNow = newList.get(0);

        for (int i=1; i < newList.size(); i++) {

            resultTillNow.retainAll(newList.get(i));
        }

        for(String positionsAfterJoin: resultTillNow) {
            String[] split = positionsAfterJoin.split("#");

            TID tid = new TID(leftColumnarFile.numColumns, Integer.parseInt(split[0]));
            Tuple tuple = leftColumnarFile.getTuple(tid);

            if(PredEval.Eval(outerExp, tuple, null, leftColumnarFile.getAttributes(), null)) {
                TID tid1 = new TID(rightColumnarFile.numColumns, Integer.parseInt(split[1]));
                Tuple tuple1 = rightColumnarFile.getTuple(tid1);
                if(PredEval.Eval(innerExp, tuple1, null, rightColumnarFile.getAttributes(), null)) {

                    Tuple Jtuple = new Tuple();
                    AttrType[] Jtypes=new AttrType[n_out_flds];

                    TupleUtils.setup_op_tuple(Jtuple,Jtypes,in1,len_in1,in2,len_in2,t1_str_sizes,t2_str_sizes,proj_list,n_out_flds);

                    Projection.Join(tuple, in1,
                            tuple1, in2,
                            Jtuple, proj_list, n_out_flds);
                    Jtuple.print(opAttr);
                }
            }
        }
    }

    private List<HashSet<String>> getUniqueSetFromJoin(CondExpr[] joinEquation, Columnarfile leftColumnarFile,
                                                       Columnarfile rightColumnarFile) throws Exception {

        List<HashSet<String>> uniquesList = new ArrayList<>();

        for(int i = 0; i < joinEquation.length; i++) {

            CondExpr currentCondition = joinEquation[i];

            while(currentCondition != null) {

                FldSpec symbol = currentCondition.operand1.symbol;
                offset1 = symbol.offset;

                offsets.get(0).add(offset1);

                HashMap<String, BitMapFile> allBitMaps = leftColumnarFile.getAllBitMaps();

                FldSpec symbol2 = currentCondition.operand2.symbol;
                offset2 = symbol2.offset;
                offsets.get(1).add(offset2);

                HashSet<String> set1 = extractUniqueValues(offset1 - 1, allBitMaps);
                HashMap<String, BitMapFile> allRightRelationBitMaps = rightColumnarFile.getAllBitMaps();


                HashSet<String> set2 = extractUniqueValues(offset2 -1, allRightRelationBitMaps);

                set1.retainAll(set2);
                uniquesList.add(set1);

                currentCondition = currentCondition.next;
                if(currentCondition != null) {
                    joinConditions.add("OR"); // always joins are represented in CNF
                }
            }
            if(i!=0 && joinEquation[i] != null)  {
                joinConditions.add("AND");
            }
        }

        return uniquesList;
    }

    public HashSet<String> extractUniqueValues(int offset, HashMap<String, BitMapFile> allBitMaps) {

        HashSet<String> collect = allBitMaps.
                keySet()
                .stream()
                .filter(e -> {
                    String[] split = e.split("\\.");
                    if (Integer.parseInt(split[2]) == offset) {
                        return true;
                    }
                    return false;
                })
                .map(e -> e.split("\\.")[3])
                .collect(Collectors.toCollection(HashSet::new));

        return collect;
    }

    public List<List<Integer>> nestedLoop(List<List<Integer>> uniqueSets)  {
        List<List<Integer>> res = new ArrayList<>();
        nestedLoopBt(uniqueSets, 0,res, new ArrayList<>());
        return res;
    }

    private void nestedLoopBt(List<List<Integer>> uniqueSets, int index, List<List<Integer>> res, List<Integer> path) {

        if(path.size() == uniqueSets.size()) {
            ArrayList<Integer> k = new ArrayList<>(path);
            res.add(k);
            return;
        }

        List<Integer> uniqueSet = uniqueSets.get(index);
        for(Integer entry: uniqueSet) {
            path.add(entry);
            nestedLoopBt(uniqueSets, index+1, res, path);
            path.remove(path.size() - 1);
        }
    }
}
