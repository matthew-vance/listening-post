package listeningpost;

/**
 * The SBS field block shared by a decoded message and an aircraft snapshot. A field is null iff the line
 * didn't carry it. Public fields + no-arg constructor: Flink serializes this as a POJO, Jackson as snake_case.
 */
public class Payload {
    public String callsign;
    public Integer altitude; // feet
    public Double groundSpeed; // knots
    public Double track; // degrees
    public Double lat;
    public Double lon;
    public Integer verticalRate; // ft/min
    public String squawk; // string: leading zeros matter
    public Boolean alert;
    public Boolean emergency;
    public Boolean spi;
    public Boolean onGround;
}
