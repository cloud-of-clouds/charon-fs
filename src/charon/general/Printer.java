package charon.general;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

public class Printer {

	private static boolean printAuth;

	public static final String ANSI_RESET = "\u001B[0m";
	public static final String ANSI_BLACK = "\u001B[30m";
	public static final String ANSI_RED = "\u001B[31m";
	public static final String ANSI_GREEN = "\u001B[32m";
	public static final String ANSI_YELLOW = "\u001B[33m";
	public static final String ANSI_BLUE = "\u001B[34m";
	public static final String ANSI_PURPLE = "\u001B[35m";
	public static final String ANSI_CYAN = "\u001B[36m";
	public static final String ANSI_WHITE = "\u001B[37m";
	private static BufferedWriter out;

	private static void initPrinter() throws UnsupportedEncodingException{
		out = new BufferedWriter(new OutputStreamWriter(new
				FileOutputStream(java.io.FileDescriptor.out), "ASCII"), 512);
	}

	public static void println(Object output){
		if(printAuth){
			try {
				out.write(output + "\n");
				out.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void println(Object output, String cor){
		if(printAuth){

			try {
				if(cor.equals("preto")){
					out.write(ANSI_BLACK + output + "\n");
				}else if(cor.equals("branco")){
					out.write(ANSI_WHITE + output + "\n");
				}else if(cor.equals("ciao")){
					out.write(ANSI_CYAN + output + "\n");
				}else if(cor.equals("amarelo")){
					out.write(ANSI_YELLOW + output + "\n");
				}else if(cor.equals("verde")){
					out.write(ANSI_GREEN + output + "\n");
				}else if(cor.equals("vermelho")){
					out.write(ANSI_RED + output + "\n");
				}else if(cor.equals("roxo")){
					out.write(ANSI_PURPLE + output + "\n");
				}else if(cor.equals("azul")){
					out.write(ANSI_CYAN + output + "\n");
				}else if(cor.equals("Dazul")){
					out.write(ANSI_BLUE + output + "\n");
				}
				out.write(ANSI_RESET);
				out.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void print(Object output, String cor){
		if(printAuth){

			try {
				if(cor.equals("preto")){
					out.write(ANSI_BLACK + output);
				}else if(cor.equals("branco")){
					out.write(ANSI_WHITE + output);
				}else if(cor.equals("ciao")){
					out.write(ANSI_CYAN + output);
				}else if(cor.equals("amarelo")){
					out.write(ANSI_YELLOW + output);
				}else if(cor.equals("verde")){
					out.write(ANSI_GREEN + output);
				}else if(cor.equals("vermelho")){
					out.write(ANSI_RED + output);
				}else if(cor.equals("roxo")){
					out.write(ANSI_PURPLE + output);
				}else if(cor.equals("azul")){
					out.write(ANSI_CYAN + output);
				}else if(cor.equals("Dazul")){
					out.write(ANSI_BLUE + output);
				}
				out.write(ANSI_RESET);
				out.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void print(Object output){
		if(printAuth){
			try {
				out.write(output + "");
				out.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void printlnErr(Object output){
		if(printAuth){
			System.err.println(output);
			System.err.flush();
		}
	}

	public static void setPrintAuth(boolean printAuthNew){
		printAuth = printAuthNew;
		if(printAuth)
			try {
				initPrinter();
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
	}

}
