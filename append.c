#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <machbase_sqlcli.h>

#define DATA_FILE_NAME          "data.csv"
#define SPARAM_MAX_COLUMN       6
#define ERROR_CHECK_COUNT	    100

#define RC_SUCCESS          	0
#define RC_FAILURE          	-1

#define UNUSED(aVar) do { (void)(aVar); } while(0)

#define CHECK_APPEND_RESULT(aRC, aEnv, aCon, aSTMT)             \
    if( !SQL_SUCCEEDED(aRC) )                                   \
    {                                                           \
        if( checkAppendError(aEnv, aCon, aSTMT) == RC_FAILURE ) \
        {                                                       \
            ;                                                   \
        }                                                       \
    }


SQLHENV 	 gEnv;
SQLHDBC 	 gCon;

static char  gTargetIP[16]="127.0.0.1";
static int   gPortNo=5656;
static char  gTable[16]="TAG";


void printError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aMsg);
int connectDB();
void disconnectDB();
int appendOpen(SQLHSTMT aStmt);
int appendData(SQLHSTMT aStmt);
unsigned long appendClose(SQLHSTMT aStmt);


void printError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aMsg)
{
    SQLINTEGER      sNativeError;
    SQLCHAR         sErrorMsg[SQL_MAX_MESSAGE_LENGTH + 1];
    SQLCHAR         sSqlState[SQL_SQLSTATE_SIZE + 1];
    SQLSMALLINT     sMsgLength;

    if( aMsg != NULL )
    {
        printf("%s\n", aMsg);
    }

    if( SQLError(aEnv, aCon, aStmt, sSqlState, &sNativeError,
        sErrorMsg, SQL_MAX_MESSAGE_LENGTH, &sMsgLength) == SQL_SUCCESS )
    {
        printf("SQLSTATE-[%s], Machbase-[%d][%s]\n", sSqlState, sNativeError, sErrorMsg);
    }
}

int checkAppendError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt)
{
    SQLINTEGER      sNativeError;
    SQLCHAR         sErrorMsg[SQL_MAX_MESSAGE_LENGTH + 1];
    SQLCHAR         sSqlState[SQL_SQLSTATE_SIZE + 1];
    SQLSMALLINT     sMsgLength;

    if( SQLError(aEnv, aCon, aStmt, sSqlState, &sNativeError,
        sErrorMsg, SQL_MAX_MESSAGE_LENGTH, &sMsgLength) != SQL_SUCCESS )
    {
        return RC_FAILURE;
    }

    printf("SQLSTATE-[%s], Machbase-[%d][%s]\n", sSqlState, sNativeError, sErrorMsg);

    if( sNativeError != 9604 &&
        sNativeError != 9605 &&
        sNativeError != 9606 )
    {
        return RC_FAILURE;
    }

    return RC_SUCCESS;
}

void appendDumpError(SQLHSTMT    aStmt,
                 SQLINTEGER  aErrorCode,
                 SQLPOINTER  aErrorMessage,
                 SQLLEN      aErrorBufLen,
                 SQLPOINTER  aRowBuf,
                 SQLLEN      aRowBufLen)
{
    char       sErrMsg[1024] = {0, };
    char       sRowMsg[32 * 1024] = {0, };

    UNUSED(aStmt);

    if (aErrorMessage != NULL)
    {
        strncpy(sErrMsg, (char *)aErrorMessage, aErrorBufLen);
    }

    if (aRowBuf != NULL)
    {
        strncpy(sRowMsg, (char *)aRowBuf, aRowBufLen);
    }

    fprintf(stdout, "Append Error : [%d][%s]\n[%s]\n\n", aErrorCode, sErrMsg, sRowMsg);
}


int connectDB()
{
    char sConnStr[1024];

    if( SQLAllocEnv(&gEnv) != SQL_SUCCESS ) 
    {
        printf("SQLAllocEnv error\n");
        return RC_FAILURE;
    }

    if( SQLAllocConnect(gEnv, &gCon) != SQL_SUCCESS ) 
    {
        printf("SQLAllocConnect error\n");

        SQLFreeEnv(gEnv);
        gEnv = SQL_NULL_HENV;

        return RC_FAILURE;
    }

    sprintf(sConnStr,"DSN=%s;UID=SYS;PWD=MANAGER;CONNTYPE=1;PORT_NO=%d", gTargetIP, gPortNo);

    if( SQLDriverConnect( gCon, NULL,
                          (SQLCHAR *)sConnStr,
                          SQL_NTS,
                          NULL, 0, NULL,
                          SQL_DRIVER_NOPROMPT ) != SQL_SUCCESS
      )
    {

        printError(gEnv, gCon, NULL, "SQLDriverConnect error");

        SQLFreeConnect(gCon);
        gCon = SQL_NULL_HDBC;

        SQLFreeEnv(gEnv);
        gEnv = SQL_NULL_HENV;

        return RC_FAILURE;
    }

    return RC_SUCCESS;
}

void disconnectDB()
{
    if( SQLDisconnect(gCon) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, NULL, "SQLDisconnect error");
    }

    SQLFreeConnect(gCon);
    gCon = SQL_NULL_HDBC;

    SQLFreeEnv(gEnv);
    gEnv = SQL_NULL_HENV;
}

int appendOpen(SQLHSTMT aStmt)
{
    const char *sTableName = gTable;

    if( SQLAppendOpen(aStmt, (SQLCHAR *)sTableName, ERROR_CHECK_COUNT) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, aStmt, "SQLAppendOpen Error");
        return RC_FAILURE;
    }

    return RC_SUCCESS;
}


int appendData(SQLHSTMT aStmt)
{
    SQL_APPEND_PARAM sParam[SPARAM_MAX_COLUMN];
    SQLRETURN        sRC;


    FILE   *fp = NULL;

    char   sBuf[128];
    char   sTagID[40];
    char   sTS[32];
    char   sEQ[16];
    char   sTG[16];
    double sVal;
    int    sQty;

    fp = fopen(DATA_FILE_NAME, "r");

    if( !fp )
    {
        printf("file open error(%s)\n", DATA_FILE_NAME);
        exit(0);
    }

    while( fgets(sBuf, 128, fp) != NULL )
    {
        sscanf(sBuf, "%[^','],%[^','],%[^','],%lf,%d\n", sTS, sEQ, sTG, &sVal,&sQty);
       
		sTagID[0]=0;
		snprintf(sTagID, 40, "%s_%s",sEQ, sTG);
		sParam[0].mVar.mLength   = strnlen(sTagID,40);
		sParam[0].mVar.mData     = sTagID;

		sParam[1].mDateTime.mTime =  SQL_APPEND_DATETIME_STRING;
		sParam[1].mDateTime.mDateStr =  sTS;
		sParam[1].mDateTime.mFormatStr =  "YYYY-MM-DD HH24:MI:SS.mmm";

		sParam[2].mDouble        = sVal;
		sParam[3].mInteger       = sQty;

		sParam[4].mVar.mLength   = strnlen(sEQ,16);
		sParam[4].mVar.mData     = sEQ;

		sParam[5].mVar.mLength   = strnlen(sTG,16);
		sParam[5].mVar.mData     = sTG;

		sRC = SQLAppendDataV2(aStmt, sParam);
		CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);
    }

    fclose(fp);

    return RC_SUCCESS;
}


unsigned long appendClose(SQLHSTMT aStmt)
{
    unsigned long sSuccessCount = 0;
    unsigned long sFailureCount = 0;

    if( SQLAppendClose(aStmt, (SQLBIGINT *)&sSuccessCount, (SQLBIGINT *)&sFailureCount) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, aStmt, "SQLAppendClose Error");
        return RC_FAILURE;
    }

    printf("success : %ld, failure : %ld\n", sSuccessCount, sFailureCount);

    return sSuccessCount;
}

int main()
{
    SQLHSTMT    sStmt = SQL_NULL_HSTMT;

    if( connectDB() == RC_SUCCESS )
    {
        printf("connectDB success\n");
    }
    else
    {
        printf("connectDB failure\n");
        goto error;
    }


    if( SQLAllocStmt(gCon, &sStmt) != SQL_SUCCESS ) 
    {
        printError(gEnv, gCon, sStmt, "SQLAllocStmt Error");
        goto error;
    }

    if( appendOpen(sStmt) == RC_SUCCESS )
    {
        printf("appendOpen success\n");
    }
    else
    {
        printf("appendOpen failure\n");
        goto error;
    }

    if( SQLAppendSetErrorCallback(sStmt, appendDumpError) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, sStmt, "SQLAppendSetErrorCallback Error");
        goto error;
    }


    appendData(sStmt);

    appendClose(sStmt);


    if( SQLFreeStmt(sStmt, SQL_DROP) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, sStmt, "SQLFreeStmt Error");
        goto error;
    }
    sStmt = SQL_NULL_HSTMT;

    disconnectDB();

    return RC_SUCCESS;

error:
    if( sStmt != SQL_NULL_HSTMT )
    {
        SQLFreeStmt(sStmt, SQL_DROP);
        sStmt = SQL_NULL_HSTMT;
    }

    if( gCon != SQL_NULL_HDBC )
    {
        disconnectDB();
    }

    return RC_FAILURE;
}
