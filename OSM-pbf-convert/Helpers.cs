static internal class Helpers
{
    public static int CoordAsInt(double value)
    {
        return (int) (value / 180 * int.MaxValue);
    }

    public static double IntToCoord(int value)
    {
        return value * 180.0 / int.MaxValue;
    }
}