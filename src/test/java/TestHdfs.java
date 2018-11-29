import java.util.Random;


public class TestHdfs {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Random r=new Random();
		for(int i=0;i<10;i++){
		    System.out.println(new Random().nextInt(10000));
		}
		
		/*
		String hdfsUri = "hdfs://hadoop-r720-4:8020/";
		
		FSDataOutputStream out = null;
		try {
			FileSystem fs = FileSystem.get(new URI(hdfsUri),
					new Configuration(), "hadoop");

			

			String currDatetime = DateUtil.getCurrDateTime();
			//first,write a hidden file
			String writingFilename =  "/examples/."+  currDatetime + ".txt";
			Path writingfile = new Path(writingFilename);
			out = fs.create(writingfile);
			
			if(!fs.exists(writingfile)){
				out = fs.create(writingfile);
			}
			
			if(fs.exists(writingfile)){
				
				System.out.println("Starting to write file: " + writingFilename);
				
				out.writeBytes("test\r\n");
				
				
				out.close();
				out = null;
				System.out.println("Finished writing file: " + writingFilename);

				//rename
				String dstFilename = "/examples/" + currDatetime + ".txt";
				fs.rename(writingfile, new Path(dstFilename));
				System.out.println("Renamed file: " + writingFilename + " to " + dstFilename);
			}
			else{
				System.out.println("File create fail: " + writingFilename);
			}
	

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				out = null;
			}
		}
*/
	}

}
