package org.jboss.cache;

import java.util.StringTokenizer;

/**
 * Contains version information about this release of TreeCache.
 *
 * @author Bela Ban
 * @version $Id: Version.java 5474 2008-03-27 21:49:43Z jiwils $
 */
public class Version
{
   public static final String version = "1.4.1.SP9";
   public static final String codename = "Cayenne";
   public static byte[] version_id = {'0', '1', '4', '1', 'S', 'P', '9'};
   public static final String cvs = "$Id: Version.java 5474 2008-03-27 21:49:43Z jiwils $";

   private static final int MAJOR_SHIFT = 11;
   private static final int MINOR_SHIFT = 6;
   private static final int MAJOR_MASK = 0x00f800;
   private static final int MINOR_MASK = 0x0007c0;
   private static final int PATCH_MASK = 0x00003f;

   private static final short SHORT_1_2_3 = encodeVersion(1, 2, 3);
   private static final short SHORT_1_2_4_SP2 = encodeVersion(1, 2, 4);

   /**
    * Prints version information.
    */
   public static void main(String[] args)
   {
      System.out.println("\nVersion: \t" + version);
      System.out.println("Codename: \t" + codename);
      System.out.println("CVS:      \t" + cvs);
      System.out.println("History:  \t(see http://jira.jboss.com/jira/browse/JBCACHE for details)\n");
   }

   /**
    * Returns version information as a string.
    */
    public static String printVersion() {
        return "JBossCache '" + codename + "' " + version + "[ " + cvs + "]";
   }

   public static String printVersionId(byte[] v, int len)
   {
      StringBuffer sb = new StringBuffer();
      if (v != null)
      {
         if (len <= 0)
            len = v.length;
         for (int i = 0; i < len; i++)
            sb.append((char) v[i]);
      }
      return sb.toString();
   }

   public static String printVersionId(byte[] v)
   {
      StringBuffer sb = new StringBuffer();
      if (v != null)
      {
         for (int i = 0; i < v.length; i++)
            sb.append((char) v[i]);
      }
      return sb.toString();
   }


   public static boolean compareTo(byte[] v)
   {
      if (v == null)
         return false;
      if (v.length < version_id.length)
         return false;
      for (int i = 0; i < version_id.length; i++)
      {
         if (version_id[i] != v[i])
            return false;
      }
      return true;
   }

   public static int getLength()
   {
      return version_id.length;
   }

   public static short getVersionShort()
   {
      return getVersionShort(version);
   }

   public static short getVersionShort(String versionString)
   {
      if (versionString == null)
         throw new IllegalArgumentException("versionString is null");

      // Special cases for version prior to 1.2.4.SP2
      if ("1.2.4".equals(versionString))
         return 124;
      else if ("1.2.4.SP1".equals(versionString))
         return 1241;

      StringTokenizer tokenizer = new StringTokenizer(versionString, ".");

      int major = 0;
      int minor = 0;
      int patch = 0;

      if (tokenizer.hasMoreTokens())
         major = Integer.parseInt(tokenizer.nextToken());
      if (tokenizer.hasMoreTokens())
         minor = Integer.parseInt(tokenizer.nextToken());
      if (tokenizer.hasMoreTokens())
         patch = Integer.parseInt(tokenizer.nextToken());

      return encodeVersion(major, minor, patch);
   }

   public static String getVersionString(short versionShort)
   {
      if (versionShort == SHORT_1_2_4_SP2)
         return "1.2.4.SP2";

      switch (versionShort)
      {
         case 124:
            return "1.2.4";
         case 1241:
            return "1.2.4.SP1";
         default:
            return decodeVersion(versionShort);
      }
   }

   public static short encodeVersion(int major, int minor, int patch)
   {
      short version = (short) ((major << MAJOR_SHIFT)
              + (minor << MINOR_SHIFT)
              + patch);
      return version;
   }

   public static String decodeVersion(short version)
   {
      int major = (version & MAJOR_MASK) >> MAJOR_SHIFT;
      int minor = (version & MINOR_MASK) >> MINOR_SHIFT;
      int patch = (version & PATCH_MASK);
      String versionString = major + "." + minor + "." + patch;
      return versionString;
   }

   public static boolean isBefore124(short version)
   {
      return (version > 1241 && version <= SHORT_1_2_3);
   }
}
