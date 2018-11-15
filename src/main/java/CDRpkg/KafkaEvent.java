package CDRpkg;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.table.descriptors.Kafka;

/**
 * The event type used in the {@link KafkaDetectRepeatedCallsUnique}.
 *
 * <p>This is a Java POJO, which Flink recognizes and will allow "by-name" field referencing
 * when keying a {@link org.apache.flink.streaming.api.datastream.DataStream} of such a type.
 * For a demonstration of this, see the code in {@link KafkaDetectRepeatedCallsUnique}.
 */
public class KafkaEvent {

    private int answeredcallind;
    private String anumber;
    private String bnumber;
    private long callduration;
    private String callserialnumber;
    private long callsetupduration;
    private String chargingcase;
    private String chargingcasedesc;
    private String destcountrycode;
    private String destcountrygeohash;
    private String destcountryname;
    private String destcountryshortname;
    private String destregion;
    private String egresscarriername;
    private String egresstrunkname;
    private String egresstrunktype;
    private String gatewayname;
    private String ingresscarriername;
    private String ingresstrunkname;
    private String ingresstrunktype;
    private String manumber;
    private String mbnumber;
    private String mnpcalldesc;
    private String mnpcallid;
    private String netoperatorcat;
    private String netoperatorname;
    private String netoperatorprefix;
    private int releasecause;
    private String releasecausedesc;
    private int releaseparty;
    private String releasepartydesc;
    private int successfulcallind;
    private String terminationcode;
    private String timestamp;
    private String trunkgroupin;
    private String trunkgroupout;
    private String callresult;
    private String estimestamp;
    private int callattemptind;
    private String startstamp;
    private String esstartstamp;
    private String endstamp;
    private int rflag;
    private String dummyendfield;

    public KafkaEvent() {
    }

    public KafkaEvent(int answeredcallind, String anumber, String bnumber, long callduration, String callserialnumber, long callsetupduration, String chargingcase, String chargingcasedesc, String destcountrycode, String destcountrygeohash, String destcountryname, String destcountryshortname, String destregion, String egresscarriername, String egresstrunkname, String egresstrunktype, String gatewayname, String ingresscarriername, String ingresstrunkname, String ingresstrunktype, String manumber, String mbnumber, String mnpcalldesc, String mnpcallid, String netoperatorcat, String netoperatorname, String netoperatorprefix, int releasecause, String releasecausedesc, int releaseparty, String releasepartydesc, int successfulcallind, String terminationcode, String timestamp, String trunkgroupin, String trunkgroupout, String callresult, String estimestamp, int callattemptind, String startstamp, String esstartstamp, String endstamp, int rflag, String dummyendfield) {
        this.answeredcallind = answeredcallind;
        this.anumber = anumber;
        this.bnumber = bnumber;
        this.callduration = callduration;
        this.callserialnumber = callserialnumber;
        this.callsetupduration = callsetupduration;
        this.chargingcase = chargingcase;
        this.chargingcasedesc = chargingcasedesc;
        this.destcountrycode = destcountrycode;
        this.destcountrygeohash = destcountrygeohash;
        this.destcountryname = destcountryname;
        this.destcountryshortname = destcountryshortname;
        this.destregion = destregion;
        this.egresscarriername = egresscarriername;
        this.egresstrunkname = egresstrunkname;
        this.egresstrunktype = egresstrunktype;
        this.gatewayname = gatewayname;
        this.ingresscarriername = ingresscarriername;
        this.ingresstrunkname = ingresstrunkname;
        this.ingresstrunktype = ingresstrunktype;
        this.manumber = manumber;
        this.mbnumber = mbnumber;
        this.mnpcalldesc = mnpcalldesc;
        this.mnpcallid = mnpcallid;
        this.netoperatorcat = netoperatorcat;
        this.netoperatorname = netoperatorname;
        this.netoperatorprefix = netoperatorprefix;
        this.releasecause = releasecause;
        this.releasecausedesc = releasecausedesc;
        this.releaseparty = releaseparty;
        this.releasepartydesc = releasepartydesc;
        this.successfulcallind = successfulcallind;
        this.terminationcode = terminationcode;
        this.timestamp = timestamp;
        this.trunkgroupin = trunkgroupin;
        this.trunkgroupout = trunkgroupout;
        this.callresult = callresult;
        this.estimestamp = estimestamp;
        this.callattemptind = callattemptind;
        this.startstamp = startstamp;
        this.esstartstamp = esstartstamp;
        this.endstamp = endstamp;
        this.rflag = rflag;
        this.dummyendfield = dummyendfield;
    }

    public static KafkaEvent fromString(String eventStr) {
        String[] split = eventStr.split(",");
//        System.out.println("============================");
//        System.out.println("Kafka Message: " + eventStr);
//        System.out.println("============================");
        return new KafkaEvent(
                Integer.valueOf(split[0]),
                split[1],
                split[2],
                Long.valueOf(split[3]),
                split[4],
                Long.valueOf(split[5]),
                split[6],
                split[7],
                split[8],
                split[9],
                split[10],
                split[11],
                split[12],
                split[13],
                split[14],
                split[15],
                split[16],
                split[17],
                split[18],
                split[19],
                split[20],
                split[21],
                split[22],
                split[23],
                split[24],
                split[25],
                split[26],
                Integer.valueOf(split[27]),
                split[28],
                Integer.valueOf(split[29]),
                split[30],
                Integer.valueOf(split[31]),
                split[32],
                split[33],
                split[34],
                split[35],
                split[36],
                split[37],
                Integer.valueOf(split[38]),
                split[39],
                split[40],
                split[41],
                Integer.valueOf(split[42]),
                split[43]
                );
    }


    @Override
    public String toString() {
        return
                answeredcallind +
                "," + anumber +
                "," + bnumber +
                "," + callduration +
                "," + callserialnumber +
                "," + callsetupduration +
                "," + chargingcase +
                "," + chargingcasedesc +
                "," + destcountrycode +
                "," + destcountrygeohash +
                "," + destcountryname +
                "," + destcountryshortname +
                "," + destregion +
                "," + egresscarriername +
                "," + egresstrunkname +
                "," + egresstrunktype +
                "," + gatewayname +
                "," + ingresscarriername +
                "," + ingresstrunkname +
                "," + ingresstrunktype +
                "," + manumber +
                "," + mbnumber +
                "," + mnpcalldesc +
                "," + mnpcallid +
                "," + netoperatorcat +
                "," + netoperatorname +
                "," + netoperatorprefix +
                "," + releasecause +
                "," + releasecausedesc +
                "," + releaseparty +
                "," + releasepartydesc +
                "," + successfulcallind +
                "," + terminationcode +
                "," + timestamp +
                "," + trunkgroupin +
                "," + trunkgroupout +
                "," + callresult +
                "," + estimestamp +
                "," + callattemptind +
                "," + startstamp +
                "," + esstartstamp +
                "," + endstamp +
                "," + rflag +
                "," + dummyendfield;
    }

    public int getAnsweredcallind() {
        return answeredcallind;
    }

    public void setAnsweredcallind(int answeredcallind) {
        this.answeredcallind = answeredcallind;
    }

    public String getAnumber() {
        return anumber;
    }

    public void setAnumber(String anumber) {
        this.anumber = anumber;
    }

    public String getBnumber() {
        return bnumber;
    }

    public void setBnumber(String bnumber) {
        this.bnumber = bnumber;
    }

    public long getCallduration() {
        return callduration;
    }

    public void setCallduration(long callduration) {
        this.callduration = callduration;
    }

    public String getCallserialnumber() {
        return callserialnumber;
    }

    public void setCallserialnumber(String callserialnumber) {
        this.callserialnumber = callserialnumber;
    }

    public long getCallsetupduration() {
        return callsetupduration;
    }

    public void setCallsetupduration(long callsetupduration) {
        this.callsetupduration = callsetupduration;
    }

    public String getChargingcase() {
        return chargingcase;
    }

    public void setChargingcase(String chargingcase) {
        this.chargingcase = chargingcase;
    }

    public String getChargingcasedesc() {
        return chargingcasedesc;
    }

    public void setChargingcasedesc(String chargingcasedesc) {
        this.chargingcasedesc = chargingcasedesc;
    }

    public String getDestcountrycode() {
        return destcountrycode;
    }

    public void setDestcountrycode(String destcountrycode) {
        this.destcountrycode = destcountrycode;
    }

    public String getDestcountrygeohash() {
        return destcountrygeohash;
    }

    public void setDestcountrygeohash(String destcountrygeohash) {
        this.destcountrygeohash = destcountrygeohash;
    }

    public String getDestcountryname() {
        return destcountryname;
    }

    public void setDestcountryname(String destcountryname) {
        this.destcountryname = destcountryname;
    }

    public String getDestcountryshortname() {
        return destcountryshortname;
    }

    public void setDestcountryshortname(String destcountryshortname) {
        this.destcountryshortname = destcountryshortname;
    }

    public String getDestregion() {
        return destregion;
    }

    public void setDestregion(String destregion) {
        this.destregion = destregion;
    }

    public String getEgresscarriername() {
        return egresscarriername;
    }

    public void setEgresscarriername(String egresscarriername) {
        this.egresscarriername = egresscarriername;
    }

    public String getEgresstrunkname() {
        return egresstrunkname;
    }

    public void setEgresstrunkname(String egresstrunkname) {
        this.egresstrunkname = egresstrunkname;
    }

    public String getEgresstrunktype() {
        return egresstrunktype;
    }

    public void setEgresstrunktype(String egresstrunktype) {
        this.egresstrunktype = egresstrunktype;
    }

    public String getGatewayname() {
        return gatewayname;
    }

    public void setGatewayname(String gatewayname) {
        this.gatewayname = gatewayname;
    }

    public String getIngresscarriername() {
        return ingresscarriername;
    }

    public void setIngresscarriername(String ingresscarriername) {
        this.ingresscarriername = ingresscarriername;
    }

    public String getIngresstrunkname() {
        return ingresstrunkname;
    }

    public void setIngresstrunkname(String ingresstrunkname) {
        this.ingresstrunkname = ingresstrunkname;
    }

    public String getIngresstrunktype() {
        return ingresstrunktype;
    }

    public void setIngresstrunktype(String ingresstrunktype) {
        this.ingresstrunktype = ingresstrunktype;
    }

    public String getManumber() {
        return manumber;
    }

    public void setManumber(String manumber) {
        this.manumber = manumber;
    }

    public String getMbnumber() {
        return mbnumber;
    }

    public void setMbnumber(String mbnumber) {
        this.mbnumber = mbnumber;
    }

    public String getMnpcalldesc() {
        return mnpcalldesc;
    }

    public void setMnpcalldesc(String mnpcalldesc) {
        this.mnpcalldesc = mnpcalldesc;
    }

    public String getMnpcallid() {
        return mnpcallid;
    }

    public void setMnpcallid(String mnpcallid) {
        this.mnpcallid = mnpcallid;
    }

    public String getNetoperatorcat() {
        return netoperatorcat;
    }

    public void setNetoperatorcat(String netoperatorcat) {
        this.netoperatorcat = netoperatorcat;
    }

    public String getNetoperatorname() {
        return netoperatorname;
    }

    public void setNetoperatorname(String netoperatorname) {
        this.netoperatorname = netoperatorname;
    }

    public String getNetoperatorprefix() {
        return netoperatorprefix;
    }

    public void setNetoperatorprefix(String netoperatorprefix) {
        this.netoperatorprefix = netoperatorprefix;
    }

    public int getReleasecause() {
        return releasecause;
    }

    public void setReleasecause(int releasecause) {
        this.releasecause = releasecause;
    }

    public String getReleasecausedesc() {
        return releasecausedesc;
    }

    public void setReleasecausedesc(String releasecausedesc) {
        this.releasecausedesc = releasecausedesc;
    }

    public int getReleaseparty() {
        return releaseparty;
    }

    public void setReleaseparty(int releaseparty) {
        this.releaseparty = releaseparty;
    }

    public String getReleasepartydesc() {
        return releasepartydesc;
    }

    public void setReleasepartydesc(String releasepartydesc) {
        this.releasepartydesc = releasepartydesc;
    }

    public int getSuccessfulcallind() {
        return successfulcallind;
    }

    public void setSuccessfulcallind(int successfulcallind) {
        this.successfulcallind = successfulcallind;
    }

    public String getTerminationcode() {
        return terminationcode;
    }

    public void setTerminationcode(String terminationcode) {
        this.terminationcode = terminationcode;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getTrunkgroupin() {
        return trunkgroupin;
    }

    public void setTrunkgroupin(String trunkgroupin) {
        this.trunkgroupin = trunkgroupin;
    }

    public String getTrunkgroupout() {
        return trunkgroupout;
    }

    public void setTrunkgroupout(String trunkgroupout) {
        this.trunkgroupout = trunkgroupout;
    }

    public String getCallresult() {
        return callresult;
    }

    public void setCallresult(String callresult) {
        this.callresult = callresult;
    }

    public String getEstimestamp() {
        return estimestamp;
    }

    public void setEstimestamp(String estimestamp) {
        this.estimestamp = estimestamp;
    }

    public int getCallattemptind() {
        return callattemptind;
    }

    public void setCallattemptind(int callattemptind) {
        this.callattemptind = callattemptind;
    }

    public String getStartstamp() {
        return startstamp;
    }

    public void setStartstamp(String startstamp) {
        this.startstamp = startstamp;
    }

    public String getEsstartstamp() {
        return esstartstamp;
    }

    public void setEsstartstamp(String esstartstamp) {
        this.esstartstamp = esstartstamp;
    }

    public String getEndstamp() {
        return endstamp;
    }

    public void setEndstamp(String endstamp) {
        this.endstamp = endstamp;
    }

    public int getRflag() {
        return rflag;
    }

    public void setRflag(int rflag) {
        this.rflag = rflag;
    }

    public String getDummyendfield() {
        return dummyendfield;
    }

    public void setDummyendfield(String dummyendfield) {
        this.dummyendfield = dummyendfield;
    }

    @Override
    public boolean equals(Object o) {
        // If the object is compared with itself then return true
        if (o == this) {
            return true;
        }

        /* Check if o is an instance of Complex or not
          "null instanceof [type]" also returns false */
        if (!(o instanceof KafkaEvent)) {
            return false;
        }

        // typecast o to Complex so that we can compare data members
        KafkaEvent c = (KafkaEvent) o;

        return c.getCallserialnumber().equals(getCallserialnumber())
                && c.getGatewayname().equals(getGatewayname());

    }
}