using System;

static internal class Helpers
{
    public static int CoordAsInt(double value)
    {
        return (int) (value / 180 * Int32.MaxValue);
    }
}